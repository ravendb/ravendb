@echo off

powershell -NoProfile -ExecutionPolicy Bypass -Command "& '%~dp0\git_setup.ps1' %*
