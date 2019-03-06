#!/bin/bash
echo "Install ravendb.service"
echo "======================="
echo ""
echo "Replace RAVENDB_USERNAME and RAVENDB_PATH as needed in /etc/systemd/system/ravendb.service"
echo "If prompt, enter su credentials..."
sudo cp ravendb.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable ravendb.service

echo " "
echo "Starting service..."
sudo systemctl start ravendb.service
echo " "
sudo systemctl status ravendb.service
echo " "
echo "For details try journal -u ravendb.service"
echo ""


