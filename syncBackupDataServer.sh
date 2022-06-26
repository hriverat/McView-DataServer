#!/bin/bash
rclone sync -v "/home/pi/Desktop/DataHistorisationAPP/Setup_McViewDataHistorisation.csv" "sharepointmv:General/08 - Softwares/McView DataServer/Input/Setup DataServer"
rclone sync -v "/home/pi/Desktop/DataHistorisationAPP/Dictionaries" "sharepointmv:General/08 - Softwares/McView DataServer/Input/Dictionaries"
rclone sync -v "/home/pi/Desktop/DataHistorisationAPP/IoT Gateway Status" "sharepointmv:General/08 - Softwares/McView DataServer/Output/IoT Gateway Status"