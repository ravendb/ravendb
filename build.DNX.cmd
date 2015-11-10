@echo off

powershell -NoProfile -ExecutionPolicy Bypass -Command "& '%~dp0\build.DNX.ps1' %*
