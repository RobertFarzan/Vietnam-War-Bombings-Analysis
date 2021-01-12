#!/bin/bash

MAX_OPT=4

re='^[0-9]+$'

#color codes
BLUE="\033[1;34m"
RED="\033[1;31m"
GREEN="\033[1;32m"
WHITE="\033[1;37m"
PURPLE="\033[1;35m"
ENDC="\033[0m"

echo -e "\n\t${WHITE}*****************************************${ENDC}"
echo -e "\t${WHITE}*${ENDC}${GREEN}***************************************${WHITE}*${ENDC}"
echo -e "\t${WHITE}*${ENDC}${GREEN}*                                     *${WHITE}*${ENDC}"
echo -e "\t${WHITE}*${ENDC}${GREEN}*    ${WHITE}INSTALLATION OF DEPENDENCIES     ${GREEN}*${WHITE}*${ENDC}"
echo -e "\t${WHITE}*${ENDC}${GREEN}*                                     *${WHITE}*${ENDC}"
echo -e "\t${WHITE}*${ENDC}${GREEN}***************************************${WHITE}*${ENDC}"
echo -e "\t${WHITE}*****************************************${ENDC}\n"

echo -e "${RED}**WARNING: This installation script does not include Spark installation**${ENDC}\n"
echo -e "${RED}**This program is only available for Python 3 versions**${ENDC}\n"
echo -e "\n\t${PURPLE}Options: \n ${ENDC}"
echo -e "\t($BLUE 1 $ENDC) Install Matplotlib "
echo -e "\t($BLUE 2 $ENDC) Install Pandas "
echo -e "\t($BLUE 3 $ENDC) Install Plotly "
echo -e "\t($BLUE 4 $ENDC) Install ALL DEPENDENCIES "
echo -e "\t($RED 0 $ENDC) Exit "
 

echo -ne "\nChoose an option number: ${WHITE}"
read opt
echo -ne "${ENDC}"

while ! [[ $opt =~ $re ]] || [ $opt -gt $MAX_OPT -o $opt -lt 0 ]
do
	echo -ne "${RED}Wrong input.${ENDC} Choose another option number: ${WHITE}"
	read opt
	echo -ne "${ENDC}"
done

if [ $opt -eq 0 ] ;then
	exit 0
fi

if [ $opt -eq 1 ] ;then
	
    sudo apt-get -y install python3-tk
fi

#matplotlib 3.3.3
#plotly 4.14.1 https://pypi.org/project/plotly/
#tkinter 8.6 https://riptutorial.com/tkinter/example/3206/installation-or-setup
#pandas 1.1.5
