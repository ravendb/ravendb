@echo off

powershell -NoProfile -ExecutionPolicy Bypass -Command "& '%~dp0\bisect.ps1' %*
