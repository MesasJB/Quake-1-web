@echo off
setlocal
rem ===== Compilador de Quake (Qwasm) para navegador =====
rem Asume que Emscripten esta en C:\Users\shit3\emsdk
call C:\Users\shit3\emsdk\emsdk_env.bat
cd /d "%~dp0"

if not exist id1 mkdir id1
if not exist id1\pak0.pak (
  if exist pak0.pak (
    copy /Y pak0.pak id1\pak0.pak >nul
  ) else (
    echo.
    echo ERROR: no encuentro pak0.pak.
    echo Copialo en esta carpeta: %~dp0
    pause
    exit /b 1
  )
)

echo.
echo Compilando Quake... la primera vez puede tardar unos minutos.
echo.

call emcc -O3 -ffast-math -funroll-loops -fomit-frame-pointer -w -Wno-implicit-function-declaration -Wno-implicit-int -Wno-int-conversion -Wno-incompatible-pointer-types -Wno-incompatible-function-pointer-types -Wno-return-type -sUSE_SDL=2 -DSDL chase.c cl_demo.c cl_input.c cl_main.c cl_parse.c cl_tent.c cmd.c common.c console.c crc.c cvar.c host.c host_cmd.c keys.c mathlib.c menu.c net_bsd.c net_dgrm.c net_loop.c net_main.c net_udp.c net_web.c net_vcr.c pr_cmds.c pr_edict.c pr_exec.c r_part.c sbar.c snd_dma.c snd_mem.c snd_mix.c sv_main.c sv_move.c sv_phys.c sv_user.c view.c wad.c world.c zone.c d_edge.c d_fill.c d_init.c d_modech.c d_part.c d_polyse.c d_scan.c d_sky.c d_sprite.c d_surf.c d_vars.c d_zpoint.c draw.c model.c nonintel.c r_aclip.c r_alias.c r_bsp.c r_draw.c r_edge.c r_efrag.c r_light.c r_main.c r_misc.c r_sky.c r_sprite.c r_surf.c r_vars.c screen.c cd_null.c snd_sdl.c sys_sdl.c vid_sdl.c -sINITIAL_MEMORY=64MB -sTOTAL_STACK=2MB -sALLOW_MEMORY_GROWTH --preload-file=id1 --shell-file shell.html -lidbfs.js -o index.html

if errorlevel 1 (
  echo.
  echo === HUBO UN ERROR DE COMPILACION. Copia las ultimas lineas y mandalas. ===
  pause
  exit /b 1
)

echo.
echo === LISTO ===
echo Para jugar (solo o con otros), en una ventana de cmd escribi:
echo    cd /d "%~dp0"
echo    node server\server.js
echo y entra a http://localhost:8765 (con Ctrl+F5)
echo (Necesitas Node.js. Ese servidor sirve el juego Y las salas online.)
echo.
pause
