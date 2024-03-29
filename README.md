# Vietnam War Bombings Analysis

![](https://github.com/RobertFarzan/Vietnam-War-Bombings-Analysis/blob/gh-pages/assets/img/header-bg.jpg)

## Description

__Vietnam War Bombings Analysis__ is a project which is intended to analyze a large amount of data from a dataset containing valuable information about the Vietnam War bombings, using Batch Data Processing to draw some results in the form of charts and graphs, that will help people verify whether this conclusions match with the real history.

We took the [dataset from Kaggle](https://www.kaggle.com/usaf/vietnam-war-bombing-operations) which data was provided by the [Defense Digital Service](https://dds.mil/) of the **US Department of Defense**.

## Pre-requisites

 - **Ubuntu** 18.04 (compatible with other systems as well)
 - **Python** 3.6.9 (at least)
 - **Apache Spark** 3.0.1 (at least)
 
## 1. :floppy_disk: How to install the project

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

> :warning: **These are the minimum versions we used during the development. Installing older versions of these dependencies might cause the program to stop working properly, but installing newest versions should not be a problem**  

### c. Download the dataset from Kaggle.

Downloading the dataset from this GitHub repository is no longer available. Instead, [download the dataset from Kaggle](https://www.kaggle.com/usaf/vietnam-war-bombing-operations).

## <br/>2. :rocket: How to run and use the program 

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

> :warning:  Choosing to use this run script **is more suitable for running in local mode**

### b. Running it with `spark-submit`
If you might want to **do some further configuration** to run the script, such as **specify the number of cores to use** or **run the script on a AWS cluster**, this option is more suitable for you.</br>

The first task to carry out is to **set Spark to use Python3**. In many systems, Spark still uses Python 2.7 by default if installed. To make this just type:
```
$ export PYSPARK_PYTHON=python3
```
This change is temporary. However, if you want to make it permanent, you'll have to write the previous line on your `$HOME/.profile` file:
```
$ vim $HOME/.profile
(write "export PYSPARK_PYTHON=python3" at the end)
$ source $HOME/.profile
```
Afterwards, we can finally proceed with running the program, using `spark-submit`. The most basic test case is the following:
```
$ spark-submit project_Vietnam.py [option]
```
If no number of option is provided, it will run **all of the options**.

The upside of this choice is that **you are allowed to set the number of nodes and cores to use on the running process**. To do this, you just have to specify the parameters `--num-executors` and `--executor-cores`. Remember to to write them BEFORE the script file. For instance:
```
$ spark-submit --num-executors 2 --executor-cores 4 project_Vietnam.py [option] --cluster
```
Will run the script on 2 machines, each using 4 cores.

#### Extra options
Additionally, we have set a few more options to configure in case you want to work in **cluster mode** (local mode is set by default), **change the output folder** or **the dataset file name**. Here we use how to use them:
```
optional arguments:
  -h, --help   show this help message and exit
  -f FILENAME  input data (default: THOR_Vietnam_Bombing_Operations.csv)
  -o FOLDER    output folder (default: output)
  --cluster    run in cluster mode
  -c CORES     number of cores to use (only works in local mode) (default: *)
```

> :white_check_mark:  The **-f option** is useful when you have the dataset **uploaded to an AWS S3 Bucket**. In that case, you just have to paste the link to the file on the S3 Bucket after -f

## :computer: Project website 
You can find more information about this project on our [GitHub website](https://robertfarzan.github.io/Vietnam-War-Bombings-Analysis/)


## :construction_worker: Authors
[1]:https://github.com/RobertFarzan
[2]:https://github.com/raquelpgo
[3]:https://github.com/migroble

- [Robert Farzan Rodríguez][1]
- [Raquel Pérez González de Ossuna][2]
- [Miguel Robledo Casal][3]
