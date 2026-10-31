/*
net_web.c -- driver de red para navegador.

Los navegadores no pueden usar UDP/TCP directo, asi que el multijugador va por un
servidor relay (server/server.js) a traves de un WebSocket que maneja online.js.
WebSocket ya es confiable y ordenado, asi que no hace falta el protocolo de
datagramas de Quake: cada mensaje del juego viaja tal cual.

Formato de cada paquete con el relay:   [id u32 big-endian][tipo u8][datos...]
(id = destino al enviar, origen al recibir)

Direcciones: cada jugador tiene un id numerico. Se escribe "#12" (o solo "12").
*/
#include "quakedef.h"
#include "net_web.h"

#include <emscripten.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define T_HELLO			1
#define T_ACCEPT		2
#define T_REJECT		3
#define T_RELIABLE		4
#define T_UNRELIABLE	5
#define T_CLOSE			6

#define WEB_MAXPEERS	32
#define WEB_MAXQUEUE	256
#define WEB_FRAMESIZE	(NET_MAXMESSAGE + 64)

void M_Web_Join (char *id);		// menu.c

/*============================================================================
  Puente con JavaScript (online.js). Se definen con EM_JS (fuera de cualquier
  funcion con setjmp, que es lo que Emscripten exige).
============================================================================*/
EM_JS(int, js_net_start, (void), {
	if (typeof QOnline === 'undefined') return 0;
	QOnline.start();
	return 1;
});

EM_JS(int, js_net_recv, (unsigned char *buf, int max), {
	var p = QOnline.shift();
	if (!p) return 0;
	if (p.length > max) return -1;
	HEAPU8.set(p, buf);
	return p.length;
});

EM_JS(int, js_net_send, (unsigned char *buf, int len, int unreliable), {
	return QOnline.send(HEAPU8.slice(buf, buf + len), unreliable);
});

EM_JS(int, js_net_myid, (void), { return QOnline.myId(); });
EM_JS(int, js_net_epoch, (void), { return QOnline.epoch(); });
EM_JS(void, js_net_listen, (int on), { QOnline.setListen(on); });
EM_JS(void, js_net_refresh, (void), { QOnline.refreshRooms(); });

EM_JS(void, js_net_room, (const char *name, const char *map, int players, int max), {
	function cstr (p) { var s = ''; for (var c; (c = HEAPU8[p++]); ) s += String.fromCharCode(c); return s; }
	QOnline.setRoom(cstr(name), cstr(map), players, max);
});

EM_JS(void, js_net_notify, (const char *msg), {
	var s = ''; for (var c; (c = HEAPU8[msg++]); ) s += String.fromCharCode(c);
	QOnline.notify(s);
});

EM_JS(int, js_net_cmd, (char *buf, int max), {
	var s = QOnline.pollCmd();
	if (s === null) return 0;
	var e = new TextEncoder().encode(s);
	var n = Math.min(e.length, max - 1);
	HEAPU8.set(e.subarray(0, n), buf);
	HEAPU8[buf + n] = 0;
	return n;
});

EM_JS(int, js_net_room_count, (void), { return QOnline.roomCount(); });

EM_JS(int, js_net_room_line, (int i, char *buf, int max), {
	var s = QOnline.roomLine(i);
	var e = new TextEncoder().encode(s);
	var n = Math.min(e.length, max - 1);
	HEAPU8.set(e.subarray(0, n), buf);
	HEAPU8[buf + n] = 0;
	return n;
});

EM_JS(void, js_ask_name, (const char *cur), {
	var s = ''; for (var c; (c = HEAPU8[cur++]); ) s += String.fromCharCode(c);
	if (typeof QOnline !== 'undefined') QOnline.askName(s);
});

/*============================================================================
  Estado
============================================================================*/
typedef struct webmsg_s
{
	struct webmsg_s	*next;
	int				type;
	int				len;
	byte			data[1];
} webmsg_t;

typedef struct
{
	qboolean		used;
	unsigned int	id;			// id del otro jugador
	qboolean		hostside;	// true: alguien se unio a mi sala
	qboolean		closed;		// el otro lado se fue (o rechazo)
	qsocket_t		*sock;		// NULL mientras el motor todavia no lo recibio
	webmsg_t		*head, *tail;
	int				count;
} webpeer_t;

static webpeer_t	peers[WEB_MAXPEERS];
static int			web_driver = -1;
static qboolean		web_ready = false;
static qboolean		web_listening = false;
static int			epoch_seen = 0;
static unsigned int	shown_id = 0xffffffff;
static double		next_room_update = 0;
static byte			rxframe[WEB_FRAMESIZE];
static byte			txframe[WEB_FRAMESIZE];

/*============================================================================
  Utilidades
============================================================================*/
static int Web_SendFrame (unsigned int id, int type, byte *payload, int len, int unreliable)
{
	if (len < 0 || len > NET_MAXMESSAGE)
		return 0;
	txframe[0] = (id >> 24) & 0xff;
	txframe[1] = (id >> 16) & 0xff;
	txframe[2] = (id >> 8) & 0xff;
	txframe[3] = id & 0xff;
	txframe[4] = type;
	if (len)
		memcpy (txframe + 5, payload, len);
	return js_net_send (txframe, len + 5, unreliable);
}

static webpeer_t *Web_FindPeer (unsigned int id)
{
	int i;
	for (i = 0; i < WEB_MAXPEERS; i++)
		if (peers[i].used && peers[i].id == id)
			return &peers[i];
	return NULL;
}

static webpeer_t *Web_NewPeer (unsigned int id, qboolean hostside)
{
	int i;
	for (i = 0; i < WEB_MAXPEERS; i++)
	{
		if (!peers[i].used)
		{
			memset (&peers[i], 0, sizeof(webpeer_t));
			peers[i].used = true;
			peers[i].id = id;
			peers[i].hostside = hostside;
			return &peers[i];
		}
	}
	return NULL;
}

static void Web_FreePeer (webpeer_t *p)
{
	webmsg_t *m, *next;
	for (m = p->head; m; m = next)
	{
		next = m->next;
		free (m);
	}
	memset (p, 0, sizeof(webpeer_t));
}

static void Web_Reject (unsigned int id, char *reason)
{
	Web_SendFrame (id, T_REJECT, (byte *)reason, strlen(reason), 0);
}

// acepta "#12", "12" o "#12:26000"
static qboolean Web_ParseId (char *s, unsigned int *id)
{
	unsigned long v = 0;
	int digits = 0;

	if (*s == '#')
		s++;
	while (*s >= '0' && *s <= '9' && digits < 9)
	{
		v = v * 10 + (*s - '0');
		s++;
		digits++;
	}
	if (!digits || v == 0)
		return false;
	if (*s && *s != ':')
		return false;
	*id = (unsigned int)v;
	return true;
}

static void Web_Queue (webpeer_t *p, int type, byte *data, int len)
{
	webmsg_t *m;

	if (p->closed)
		return;
	if (p->count >= WEB_MAXQUEUE && type == T_UNRELIABLE)
		return;		// el motor no esta leyendo (ej. cargando un mapa): descartamos los no confiables
	m = (webmsg_t *)malloc (sizeof(webmsg_t) + len);
	if (!m)
		return;
	m->next = NULL;
	m->type = type;
	m->len = len;
	if (len)
		memcpy (m->data, data, len);
	if (p->tail)
		p->tail->next = m;
	else
		p->head = m;
	p->tail = m;
	p->count++;
}

/*============================================================================
  Recibe todo lo que llego por el WebSocket y lo reparte
============================================================================*/
static void Web_Pump (void)
{
	int			n, iter, type, len;
	unsigned int	id;
	webpeer_t	*p;
	char		reason[96];

	if (!web_ready)
		return;

	for (iter = 0; iter < 512; iter++)
	{
		n = js_net_recv (rxframe, sizeof(rxframe));
		if (n == 0)
			break;
		if (n < 5)
			continue;

		id = ((unsigned int)rxframe[0] << 24) | (rxframe[1] << 16) | (rxframe[2] << 8) | rxframe[3];
		type = rxframe[4];
		len = n - 5;

		switch (type)
		{
		case T_HELLO:		// alguien quiere entrar a mi sala
			if (Web_FindPeer (id))
				break;
			if (!web_listening)
			{
				Web_Reject (id, "La sala no esta abierta.");
				break;
			}
			if (!Web_NewPeer (id, true))
				Web_Reject (id, "Sala llena.");
			break;

		case T_ACCEPT:
			break;

		case T_REJECT:
			p = Web_FindPeer (id);
			if (p)
			{
				if (len > (int)sizeof(reason) - 1)
					len = sizeof(reason) - 1;
				memcpy (reason, rxframe + 5, len);
				reason[len] = 0;
				Con_Printf ("Conexion rechazada: %s\n", reason);
				js_net_notify (reason);
				p->closed = true;
			}
			break;

		case T_RELIABLE:
		case T_UNRELIABLE:
			p = Web_FindPeer (id);
			if (p)
				Web_Queue (p, type, rxframe + 5, len);
			break;

		case T_CLOSE:
			p = Web_FindPeer (id);
			if (p)
			{
				p->closed = true;
				if (!p->sock)		// el motor nunca llego a conocerlo
					Web_FreePeer (p);
			}
			break;
		}
	}
}

/*============================================================================
  Driver
============================================================================*/
int Web_Init (void)
{
	if (COM_CheckParm ("-noweb"))
		return -1;
	if (!js_net_start ())
	{
		Con_Printf ("Web network: online.js no esta cargado\n");
		return -1;
	}

	web_driver = net_driverlevel;
	memset (peers, 0, sizeof(peers));
	epoch_seen = js_net_epoch ();
	Q_strcpy (my_tcpip_address, "(conectando)");
	tcpipAvailable = true;
	web_ready = true;

	Con_Printf ("Web network initialized\n");
	return 0;
}

void Web_Shutdown (void)
{
	int i;

	if (!web_ready)
		return;
	for (i = 0; i < WEB_MAXPEERS; i++)
	{
		if (peers[i].used && !peers[i].closed)
			Web_SendFrame (peers[i].id, T_CLOSE, NULL, 0, 0);
		if (peers[i].used)
			Web_FreePeer (&peers[i]);
	}
	js_net_listen (0);
	web_listening = false;
	web_ready = false;
}

void Web_Listen (qboolean state)
{
	if (!web_ready)
		return;
	web_listening = state;
	if (state)
	{
		if (Q_strcmp (hostname.string, "UNNAMED") == 0)
		{
			char name[32];
			sprintf (name, "Sala %s", cl_name.string);
			name[15] = 0;
			Cvar_Set ("hostname", name);
		}
		next_room_update = 0;		// anunciar la sala ya mismo
	}
	js_net_listen (state);
}

// "Search for local games": lista las salas que informa el servidor
void Web_SearchForHosts (qboolean xmit)
{
	int			i, n, count;
	char		line[160], *f[5], *s;
	int			k;

	if (!web_ready)
		return;
	if (xmit)
		js_net_refresh ();

	count = js_net_room_count ();
	for (i = 0; i < count; i++)
	{
		if (hostCacheCount == HOSTCACHESIZE)
			break;
		if (js_net_room_line (i, line, sizeof(line)) <= 0)
			continue;

		// id|nombre|mapa|jugadores|maximo
		f[0] = line;
		k = 1;
		for (s = line; *s && k < 5; s++)
			if (*s == '|')
			{
				*s = 0;
				f[k++] = s + 1;
			}
		if (k < 5)
			continue;
		if ((unsigned int)atoi (f[0]) == (unsigned int)js_net_myid ())
			continue;

		n = hostCacheCount;
		sprintf (hostcache[n].cname, "#%s", f[0]);
		{
			int j, dup = 0;
			for (j = 0; j < n; j++)
				if (!strcmp (hostcache[j].cname, hostcache[n].cname))
					dup = 1;
			if (dup)
				continue;
		}
		strncpy (hostcache[n].name, f[1], 15);
		hostcache[n].name[15] = 0;
		strncpy (hostcache[n].map, f[2], 15);
		hostcache[n].map[15] = 0;
		hostcache[n].users = atoi (f[3]);
		hostcache[n].maxusers = atoi (f[4]);
		hostcache[n].driver = net_driverlevel;
		hostcache[n].ldriver = 0;
		memset (&hostcache[n].addr, 0, sizeof(hostcache[n].addr));
		hostCacheCount++;
	}
}

qsocket_t *Web_Connect (char *host)
{
	unsigned int	id;
	webpeer_t		*p;
	qsocket_t		*sock;

	if (!web_ready)
		return NULL;
	if (!Web_ParseId (host, &id))
		return NULL;

	if (!js_net_myid ())
	{
		Con_Printf ("Sin conexion con el servidor online.\n");
		js_net_notify ("Sin conexion con el servidor online.");
		return NULL;
	}
	if (id == (unsigned int)js_net_myid ())
	{
		Con_Printf ("No podes unirte a tu propia sala.\n");
		return NULL;
	}
	if (Web_FindPeer (id))
		return NULL;

	p = Web_NewPeer (id, false);
	if (!p)
		return NULL;
	sock = NET_NewQSocket ();
	if (!sock)
	{
		Web_FreePeer (p);
		return NULL;
	}
	sock->socket = (int)(p - peers);
	sock->driver = web_driver;
	sprintf (sock->address, "#%u", id);
	p->sock = sock;

	// WebSocket es confiable: damos la conexion por hecha. Si la sala la rechaza,
	// llega T_REJECT y la conexion se cae con el motivo en pantalla.
	Web_SendFrame (id, T_HELLO, NULL, 0, 0);
	return sock;
}

qsocket_t *Web_CheckNewConnections (void)
{
	int			i;
	webpeer_t	*p;
	qsocket_t	*sock;

	Web_Pump ();

	for (i = 0; i < WEB_MAXPEERS; i++)
	{
		p = &peers[i];
		if (!p->used || !p->hostside || p->sock)
			continue;
		if (p->closed)
		{
			Web_FreePeer (p);
			continue;
		}
		sock = NET_NewQSocket ();
		if (!sock)
		{
			Web_Reject (p->id, "Sala llena.");
			Web_FreePeer (p);
			continue;
		}
		sock->socket = i;
		sock->driver = web_driver;
		sprintf (sock->address, "#%u", p->id);
		p->sock = sock;
		Web_SendFrame (p->id, T_ACCEPT, NULL, 0, 0);
		return sock;
	}
	return NULL;
}

int Web_GetMessage (qsocket_t *sock)
{
	webpeer_t	*p = &peers[sock->socket];
	webmsg_t	*m;
	int			ret;

	Web_Pump ();

	if (!p->used || p->sock != sock)
		return -1;
	m = p->head;
	if (!m)
		return p->closed ? -1 : 0;

	p->head = m->next;
	if (!p->head)
		p->tail = NULL;
	p->count--;

	SZ_Clear (&net_message);
	SZ_Write (&net_message, m->data, m->len);
	ret = (m->type == T_RELIABLE) ? 1 : 2;
	free (m);
	return ret;
}

int Web_SendMessage (qsocket_t *sock, sizebuf_t *data)
{
	webpeer_t *p = &peers[sock->socket];

	if (!p->used || p->closed)
		return -1;
	if (!Web_SendFrame (p->id, T_RELIABLE, data->data, data->cursize, 0))
		return -1;
	return 1;
}

int Web_SendUnreliableMessage (qsocket_t *sock, sizebuf_t *data)
{
	webpeer_t *p = &peers[sock->socket];

	if (!p->used || p->closed)
		return -1;
	return Web_SendFrame (p->id, T_UNRELIABLE, data->data, data->cursize, 1) ? 1 : 0;
}

qboolean Web_CanSendMessage (qsocket_t *sock)
{
	webpeer_t *p = &peers[sock->socket];
	return p->used && !p->closed;
}

qboolean Web_CanSendUnreliableMessage (qsocket_t *sock)
{
	return true;
}

void Web_Close (qsocket_t *sock)
{
	webpeer_t *p = &peers[sock->socket];

	if (!p->used || p->sock != sock)
		return;
	if (!p->closed)
		Web_SendFrame (p->id, T_CLOSE, NULL, 0, 0);
	Web_FreePeer (p);
}

/*============================================================================
  Una vez por frame: paquetes, estado de la sala, comandos de la pagina
============================================================================*/
static void Web_RunCommand (char *line)
{
	char *s;

	// Solo aceptamos los comandos que usa la pagina
	if (!strncmp (line, "name ", 5))
	{
		Cbuf_AddText (va ("%s\n", line));
	}
	else if (!strncmp (line, "join ", 5))
	{
		for (s = line + 5; *s; s++)
			if (*s < '0' || *s > '9')
				return;
		M_Web_Join (line + 5);
	}
}

void WebNet_Tick (void)
{
	char			line[128];
	int				i, n, players;
	unsigned int	id;
	int				ep;
	webpeer_t		*p;

	if (!web_ready)
		return;

	Web_Pump ();

	// Se corto (o reconecto) el WebSocket: todas las conexiones se perdieron
	ep = js_net_epoch ();
	if (ep != epoch_seen)
	{
		epoch_seen = ep;
		for (i = 0; i < WEB_MAXPEERS; i++)
		{
			p = &peers[i];
			if (!p->used)
				continue;
			p->closed = true;
			if (!p->sock)
				Web_FreePeer (p);
		}
		next_room_update = 0;
	}

	// Direccion propia (se ve en el menu de TCP/IP)
	id = (unsigned int)js_net_myid ();
	if (id != shown_id)
	{
		shown_id = id;
		if (id)
			sprintf (my_tcpip_address, "#%u", id);
		else
			Q_strcpy (my_tcpip_address, "(sin conexion)");
	}

	// Anunciar mi sala para que aparezca en la lista de la pagina
	if (web_listening && Sys_FloatTime () >= next_room_update)
	{
		next_room_update = Sys_FloatTime () + 2.0;
		players = 0;
		if (sv.active)
			for (i = 0; i < svs.maxclients; i++)
				if (svs.clients[i].active)
					players++;
		js_net_room (hostname.string, sv.active ? sv.name : "", players, svs.maxclients);
	}

	// Comandos que manda la pagina (cambiar nombre, unirse a una sala)
	for (i = 0; i < 8; i++)
	{
		n = js_net_cmd (line, sizeof(line));
		if (n <= 0)
			break;
		Web_RunCommand (line);
	}
}

void WebNet_AskName (char *current)
{
	js_ask_name (current);
}
