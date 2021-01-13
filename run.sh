#!/bin/bash

export PYSPARK_PYTHON=python3
PYFILE="project_Vietnam.py"
DATASET_FILE="THOR_Vietnam_Bombing_Operations.csv"
MAX_OPT=12

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
echo -e "\t${WHITE}*${ENDC}${GREEN}*        ${WHITE}VIETNAM WAR BOMBINGS         ${GREEN}*${WHITE}*${ENDC}"
echo -e "\t${WHITE}*${ENDC}${GREEN}*                                     *${WHITE}*${ENDC}"
echo -e "\t${WHITE}*${ENDC}${GREEN}***************************************${WHITE}*${ENDC}"
echo -e "\t${WHITE}*****************************************${ENDC}"

echo -e "\n\t${PURPLE}Options: \n ${ENDC}"
echo -e "\t($BLUE ENTER $ENDC) Press enter to RUN ALL OPTIONS AT ONCE "
echo -e "\t($BLUE 1 $ENDC) Display NUMBER OF BOMBINGS by country and date "
echo -e "\t($BLUE 2 $ENDC) Display NUMBER OF MISSIONS by country and date "
echo -e "\t($BLUE 3 $ENDC) Display TOTAL NUMBER OF BOMBINGS by country "
echo -e "\t($BLUE 4 $ENDC) Display TOTAL NUMBER OF MISSIONS by country "
echo -e "\t($BLUE 5 $ENDC) Display MOST ATTACKED COUNTRIES "
echo -e "\t($BLUE 6 $ENDC) Display TYPE OF MISSIONS "
echo -e "\t($BLUE 7 $ENDC) Display MOST ATTACKED LOCATIONS MAP "
echo -e "\t($BLUE 8 $ENDC) Display MOST ATTACKED LOCATIONS MAP BY DATE"
echo -e "\t($BLUE 9 $ENDC) Display MOST USED TYPE OF AIRCRAFTS "
echo -e "\t($BLUE 10 $ENDC) Display AIRCRAFTS PER TYPE OF MISSION "
echo -e "\t($BLUE 11 $ENDC) Display AIRCRAFTS BOMBINGS PER TYPE OF MISSION "
echo -e "\t($BLUE 12 $ENDC) Display MOST COMMON TAKE-OFF LOCATIONS "
echo -e "\t($RED 0 $ENDC) Exit "
echo ""

echo -ne "Choose an option number: ${WHITE}"
read opt
echo -ne "${ENDC}"

re='^[0-9]+$'

if [ -z "${opt-}" ] ;then

    if ! [ -e $PYFILE ] ;then
	    echo -e "${RED}**ERROR: Python script file \"$PYFILE\" not found**${ENDC}"
	    exit 1
    fi

    if ! [ -e $DATASET_FILE ] ;then
	    echo -e "${RED}**ERROR: Dataset \"$DATASET_FILE\" not found**${ENDC}"
	    exit 1
    fi

    spark-submit $PYFILE
else
    while ! [[ $opt =~ $re ]] || [ $opt -gt $MAX_OPT -o $opt -lt 0 ]
    do
	    echo -ne "${RED}Wrong input.${ENDC} Choose another option number: ${WHITE}"
	    read opt
	    echo -ne "${ENDC}"
    done

    if ! [ -e $PYFILE ] ;then
	    echo -e "${RED}**ERROR: Python script file \"$PYFILE\" not found**${ENDC}"
	    exit 1
    fi

    if ! [ -e $DATASET_FILE ] ;then
	    echo -e "${RED}**ERROR: Dataset \"$DATASET_FILE\" not found**${ENDC}"
	    exit 1
    fi

    if [ $opt -eq 0 ] ;then
	    exit 0
    fi

    spark-submit $PYFILE $opt

fi 
