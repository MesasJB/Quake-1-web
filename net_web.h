// net_web.h -- driver de red "Web": juega por un servidor relay via WebSocket (ver online.js)

int			Web_Init (void);
void		Web_Listen (qboolean state);
void		Web_SearchForHosts (qboolean xmit);
qsocket_t	*Web_Connect (char *host);
qsocket_t	*Web_CheckNewConnections (void);
int			Web_GetMessage (qsocket_t *sock);
int			Web_SendMessage (qsocket_t *sock, sizebuf_t *data);
int			Web_SendUnreliableMessage (qsocket_t *sock, sizebuf_t *data);
qboolean	Web_CanSendMessage (qsocket_t *sock);
qboolean	Web_CanSendUnreliableMessage (qsocket_t *sock);
void		Web_Close (qsocket_t *sock);
void		Web_Shutdown (void);

// se llama una vez por frame (host.c)
void		WebNet_Tick (void);
// muestra la ventanita "name?:" arriba de la pagina
void		WebNet_AskName (char *current);
