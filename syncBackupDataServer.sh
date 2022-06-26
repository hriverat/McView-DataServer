#!/bin/bash
rclone sync -v "/home/pi/Desktop/DataHistorisationAPP/Dictionaries/Setup_McViewDataHistorisation.csv" "sharepointmv:General/08 - Softwares/McView DataServer/Setup DataServer"
rclone sync -v "/home/pi/Desktop/DataHistorisationAPP/Dictionaries" "sharepointmv:General/08 - Softwares/McView DataServer/Dictionaries"
rclone sync -v "/home/pi/Desktop/DataHistorisationAPP/IoT Gateway Status" "sharepointmv:General/08 - Softwares/McView DataServer/IoT Gateway Status"