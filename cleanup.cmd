@echo off

powershell -NoProfile -ExecutionPolicy Bypass -Command "& '%~dp0\cleanup.ps1' %*
