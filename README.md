# Vietnam War Bombings Analysis

![](https://github.com/RobertFarzan/Vietnam-War-Bombings-Analysis/blob/gh-pages/assets/img/header-bg.jpg)

## Description

__Vietnam War Bombings Analysis__ is a project which is intended to analyze a large amount of data from a dataset containing valuable information about the Vietnam War bombings, using Batch Data Processing to draw some results in the form of charts and graphs, that will help people verify whether this conclusions match with the real history.

We took the [dataset from Kaggle](https://www.kaggle.com/usaf/vietnam-war-bombing-operations) which data was provided by the [Defense Digital Service](https://dds.mil/) of the **US Department of Defense**.

## Prerequisites

 - **Python** 3.6.9 (at least)
 - **Apache Spark** 3.0.1 (at least)
 
## 1. How to install the project

First, you'll have to **clone or download the repository** on your local machine, by doing:
```
$ git clone https://github.com/RobertFarzan/Vietnam-War-Bombings-Analysis
```
Before start using our application, you'll have to install some extra dependencies. To do the dependencies installation, you need to have **pip3** installed. Pip is just the **Python package installer for Python3 versions**. If you don't have it already installed (you can check it by typing `$ pip3 help` or `pip3 --version`) type the following:
```
$ sudo apt-get update
$ sudo apt-get install python3-pip
```

### a. Using the installation script

The files you already downloaded includes an **installation script** that will do it all for you. Make sure you have `requirements.txt` downloaded on the same directory you run the script. You just have to run it by typing:
```
$ ./install.sh
```
If the script fails for some reason, because you're using a different platform or OS, you may want to check the installation of every single package manually, as described in the following section.

### b. Install dependencies manually

The following command installs Matplotlib, Plotly and Pandas libraries at once.
```
$ pip3 install matplotlib==3.3 plotly==4.14 pandas==1.1
```
If there's some problem during the installation, try installing them one by one and check out which one causes the failure.<br/>

> :warning: **Installing lower versions of these dependencies might cause the program to stop working properly**  

## <br/>2. How to run and use the program

To begin using the program, there are two methods, either using the **run script** that we provide you with or running it through `spark-submit`.<br/>
> :rotating_light: **You must unzip `vietnam-war-bombing-operations.zip` before running the program, otherwise it won't work** 

### a. Using the run script

To run the program with this method, you just have to run the script we provide by typing:
```
$ ./run.sh
```

Make sure you have the **dataset** `THOR_Vietnam_Bombing_Operations.csv` and the **Python script** `project_Vietnam.py` in the same directory from which you run the bash script.<br/><br/>
If everything goes as planned you should see the following display:<br/><br/>
![](https://github.com/RobertFarzan/Vietnam-War-Bombings-Analysis/blob/gh-pages/assets/img/program_display.PNG)

You can see a **fancy display with 13 options to choose**. You'll just have to type the number of option you want and after execution is done, you will find the results of the analysis on an `/output` directory.

> :heavy_exclamation_mark:  Choosing to use this run script **is more suitable for running in local mode**

### b. Running it with `spark-submit`
If you might want to **do some further configuration** tu run the script, such as **specify the number of cores to use** or **run the script on a AWS cluster**, this option is more suitable for you.</br></br>

The first task to carry out is to **set Spark to use Python3**. In many systems, Spark still uses Python 2.7 by default if installed. To make this just type:
```
$ export PYSPARK_PYTHON=python3
```
This change is temporary. However, if you want to make it permanent, you'll have to write the previous line on your `$HOME/.profile` file:
```
$ vim $HOME/.profile
(write "export PYSPARK_PYTHON=python3" at the end)
```
Afterwards, we can finally proceed with running the program, using `spark-submit`. The most basic test case is the following:
```
$ spark-submit project_Vietnam.py [option]
```
If no number of option is provided, it will run **all of the options**.
## Project website
You can find more information about this project on our [GitHub website](https://robertfarzan.github.io/Vietnam-War-Bombings-Analysis/)


## Authors
[1]:https://github.com/RobertFarzan
[2]:https://github.com/raquelpgo
[3]:https://github.com/migroble

- [Robert Farzan Rodríguez][1]
- [Raquel Pérez González de Ossuna][2]
- [Miguel Robledo Casal][3]
