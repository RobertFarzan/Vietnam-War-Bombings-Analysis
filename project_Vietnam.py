from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import *
import pandas as pd
import numpy as np
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mpt
import plotly.express as px
import plotly.graph_objects as go
import sys

conf = SparkConf().setMaster('local[*]').setAppName('VietAnalysis')
sc = SparkContext(conf = conf)
#sc.setLogLevel('WARN') #to remove most INFO messages
spark = SparkSession(sc)

list_colors = ['r', 'b', 'k', 'g', 'y', 'm', 'c']

#VietNam_1975.csv
#THOR_Vietnam_Bombing_Operations.csv
df = spark.read.csv("THOR_Vietnam_Bombing_Operations.csv", header=True, inferSchema=True)

def total_bombings_bydate():

        df2 = df.select(last_day("MSNDATE").alias("Date"), col("COUNTRYFLYINGMISSION").alias("Country"), col("NUMWEAPONSDELIVERED")) \
                .dropna() \
                .groupBy(col("Date"), col("Country")) \
                .agg(sum("NUMWEAPONSDELIVERED").alias("NumWeapons")) \
                .orderBy("Date")

        pandf = df2.toPandas()
        print(pandf)

        fig, ax = plt.subplots(figsize=(50,10))

        pandf['Date'] =  pd.to_datetime(pandf['Date'], format='%Y-%m-%d')

        for i, country in enumerate(pandf["Country"].unique()):
                ax.plot(pandf.loc[pandf["Country"] == country]["Date"], pandf.loc[pandf["Country"] == country]["NumWeapons"], "o-%c" % list_colors[i], label="%s" % country, markersize=4)
        
        ax.legend()

        #to set the right complete date on the X-axis
        ax.set_xlim(pandf['Date'].iloc[0], pandf['Date'].iloc[-1])
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
        ax.xaxis.set_minor_formatter(mdates.DateFormatter('%b'))
        ax.xaxis.set_major_locator(mdates.YearLocator(1, month=12, day=31))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

        #avoid scientific notation on Y-axis
        #ax.ticklabel_format(style='plain', axis='y')
        ax.yaxis.set_major_formatter(mpt.EngFormatter())
   
        # rotate and align the tick labels so they look better
        ax.tick_params(axis='x', which='minor', labelsize=6)
        ax.tick_params(axis='x', which='major', length=5, width=2, pad=10)
        fig.autofmt_xdate(which='both')

        plt.xlabel("Date")
        plt.ylabel("Bombs dropped")
        plt.title("Vietnam War Bombings (Monthly totals)")
        plt.grid(True)
        plt.show()


def total_missions_bydate(): 
        df2 = df.select(last_day("MSNDATE").alias("Date"), col("COUNTRYFLYINGMISSION").alias("Country")) \
                .dropna() \
                .groupBy(col("Date"), col("Country")) \
                .count() \
                .orderBy("Date")

        pandf = df2.toPandas()
        print(pandf)

        fig, ax = plt.subplots(figsize=(50,10))

        pandf['Date'] =  pd.to_datetime(pandf['Date'], format='%Y-%m-%d')

        for i, country in enumerate(pandf["Country"].unique()):
                ax.plot(pandf.loc[pandf["Country"] == country]["Date"], pandf.loc[pandf["Country"] == country]["count"], "o-%c" % list_colors[i], label="%s" % country, markersize=4)

        ax.legend()

        #to set the right complete date on the X-axis
        ax.set_xlim(pandf['Date'].iloc[0], pandf['Date'].iloc[-1])
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
        ax.xaxis.set_minor_formatter(mdates.DateFormatter('%b'))
        ax.xaxis.set_major_locator(mdates.YearLocator(1, month=12, day=31))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

        #avoid scientific notation on Y-axis
        #ax.ticklabel_format(style='plain', axis='y')
        ax.yaxis.set_major_formatter(mpt.EngFormatter())
   
        # rotate and align the tick labels so they look better
        ax.tick_params(axis='x', which='minor', labelsize=6)
        ax.tick_params(axis='x', which='major', length=5, width=2, pad=10)
        fig.autofmt_xdate(which='both')


        plt.xlabel("Date")
        plt.ylabel("Number of Missions")
        plt.title("Vietnam War Missions (Monthly totals)")
        plt.grid(True)
        plt.show()

def total_bombings_bycountry():
        df2 = df.select(col("COUNTRYFLYINGMISSION").alias("Country"), col("NUMWEAPONSDELIVERED")) \
                .dropna(subset=["Country"]) \
                .groupBy(col("Country")) \
                .agg(sum("NUMWEAPONSDELIVERED").alias("NumWeapons")) \
                .orderBy(col('NumWeapons').desc())

        pandf = df2.toPandas()
        print(pandf)

        fig, ax = plt.subplots(figsize=(9,9))

        ax.bar(pandf['Country'], pandf['NumWeapons'], color='royalblue')
        ax.legend()

        #avoid scientific notation on Y-axis
        ax.yaxis.set_major_formatter(mpt.EngFormatter())

        # rotate and align the tick labels so they look better
        plt.xticks(rotation=10)
        plt.xlabel("Countries")
        plt.ylabel("Bombs dropped")
        plt.title("Vietnam War Bombings (Totals by Country)")
        plt.show()

def total_missions_bycountry(): 
        df2 = df.select(col("COUNTRYFLYINGMISSION").alias("Country")) \
                .dropna() \
                .groupBy(col("Country")) \
                .count() \
                .orderBy(col('count').desc())

        pandf = df2.toPandas()
        print(pandf)
        fig, ax = plt.subplots(figsize=(9,9))

        ax.bar(pandf['Country'], pandf['count'])
        ax.legend()

        #avoid scientific notation on Y-axis
        ax.yaxis.set_major_formatter(mpt.EngFormatter())

        plt.xticks(rotation=10)
        plt.xlabel("Countries")
        plt.ylabel("Number of Missions")
        plt.title("Vietnam War Missions (Totals by Country)")
        plt.show()

def most_attacked_countries(): 
        df2 = df.select(col('TGTCOUNTRY').alias('TargetCountry'), col('NUMWEAPONSDELIVERED').alias('NumWeapons')) \
                .dropna() \
                .groupBy(col('TargetCountry')) \
                .agg(sum('NumWeapons').alias('TotalWeaponsDropped')) \
                .filter(col('TotalWeaponsDropped') > 0).filter(col('TargetCountry') != 'UNKNOWN') \
                .orderBy(col('TotalWeaponsDropped').desc())

        pandf = df2.toPandas()
        print(pandf)
        fig, ax = plt.subplots(figsize=(9,9))

        ax.bar(pandf['TargetCountry'], pandf['TotalWeaponsDropped'], color='royalblue')
        ax.legend()

        #avoid scientific notation on Y-axis
        #ax.ticklabel_format(style='plain', axis='y')
        ax.yaxis.set_major_formatter(mpt.EngFormatter())

        plt.xticks(rotation=10)
        plt.xlabel("Attacked Countries")
        plt.ylabel("Number of Bombs dropped")
        plt.title("Vietnam War Most Affected Countries")
        plt.show()

def mission_types():
        total_msn = df.select(col('MFUNC_DESC_CLASS'), col('MFUNC_DESC')).dropna().groupBy().count().take(1)[0][0]
        df2 = df.select(col('MFUNC_DESC_CLASS').alias('Class'), col('MFUNC_DESC').alias('Description')) \
                .dropna() \
                .groupBy(col('Description'), col('Class')) \
                .count() \
                .withColumn('Percentage', (col('count')/lit(total_msn))*100) \
                .filter(col('Percentage') >= 0.8) \
                .orderBy(col('Percentage').desc())

        pandf = df2.toPandas()
        print(pandf)
        fig, ax = plt.subplots(figsize=(20,10))

        colors = {'KINETIC':'royalblue', 'NONKINETIC':'orange'}         
        labels = list(colors.keys())
        handles = [mpatches.Patch(color=colors[label], label=label) for label in labels]
        
        colorlist = [colors[row['Class']] for i, row in pandf.iterrows()]
        ax.bar(pandf['Description'], pandf['Percentage'], color=colorlist)
        ax.legend(handles=handles)

        ax.yaxis.set_major_formatter(mpt.PercentFormatter())

        plt.xticks(rotation=15)
        plt.xlabel("Type of Mission")
        plt.ylabel("Percentage of total missions")
        plt.title("Vietnam War Type of Missions")
        plt.show()

def most_attacked_locations_map():
        
        df2 = df.select(round(col('TGTLATDD_DDD_WGS84'), 4).alias('Latitude'), round(col('TGTLONDDD_DDD_WGS84'), 4).alias('Longitude'), col('NUMWEAPONSDELIVERED').alias('Bombs dropped')) \
                .filter(col('Bombs dropped') > 0) \
                .dropna() \
                .groupBy('Latitude', 'Longitude') \
                .agg(sum('Bombs dropped').alias('Bombs dropped'), count(lit(1)).alias('Number of attacks')) \
                .filter(col('Bombs dropped') >= 150)

        pandf = df2.toPandas()    
        print(pandf)

        fig = px.scatter_mapbox(pandf,
                                lat='Latitude',
                                lon='Longitude',
                                color='Bombs dropped',
                                size='Bombs dropped',
                                hover_data=['Number of attacks'],
                                center=dict(lat=12.702571, lon=106.424241),
                                color_continuous_scale='plasma',
                                zoom=4,
                                mapbox_style="carto-positron",
                                title='Vietnam War Bombing Locations')

        fig.write_html("total_attacked_locations.html")
        fig.show()

def most_attacked_locations_map_bydate():
        
        df2 = df.select(last_day('MSNDATE').alias("Date"), round(col('TGTLATDD_DDD_WGS84'), 4).alias('Latitude'), round(col('TGTLONDDD_DDD_WGS84'), 4).alias('Longitude'), col('NUMWEAPONSDELIVERED').alias('Bombs dropped')) \
                .filter(col('Bombs dropped') > 0) \
                .dropna() \
                .groupBy('Date', 'Latitude', 'Longitude') \
                .agg(sum('Bombs dropped').alias('Bombs dropped'), count(lit(1)).alias('Number of attacks')) \
                .filter(col('Bombs dropped') >= 150) \
                .orderBy('Date') \
                .withColumn('Date', col('Date').cast(StringType()))

        pandf = df2.toPandas()    
        print(pandf)

        fig = px.scatter_mapbox(pandf, 
                                lat='Latitude',
                                lon='Longitude',
                                color='Bombs dropped',
                                size='Bombs dropped',
                                hover_data=['Number of attacks'],
                                center=dict(lat=12.702571, lon=106.424241),
                                color_continuous_scale='plasma',
                                zoom=4, 
                                mapbox_style="carto-positron",
                                animation_frame='Date',
                                title='Vietnam War Bombing Locations By Date')

        fig.write_html('attacked_locations_bydate.html')
        fig.show()

def aircraft_types():

        df2 = df.select(col('VALID_AIRCRAFT_ROOT').alias('Aircraft')) \
                .dropna() \
                .groupBy(col('Aircraft')) \
                .count() \
                .orderBy(col('count').desc()) \
                .filter(col('count') > 7000)

        pandf = df2.toPandas()
        print(pandf)
        fig, ax = plt.subplots(figsize=(20,10))

        ax.bar(pandf['Aircraft'], pandf['count'], color='royalblue')

        ax.yaxis.set_major_formatter(mpt.EngFormatter())

        plt.xticks(rotation=25)
        plt.xlabel("Type of Aircraft")
        plt.ylabel("Number of missions")
        plt.title("Vietnam War Most used Aircrafts")
        plt.show()

def aircraft_per_type_of_mission():

        df2 = df.select(col('VALID_AIRCRAFT_ROOT').alias('Aircraft'), col('MFUNC_DESC').alias('Mission')) \
                .dropna() \
                .groupBy(col('Aircraft'), col('Mission')) \
                .agg(count('Mission').alias('NumMissions')) \
                .filter(col('NumMissions') > 17000) \
                .orderBy(col('Aircraft')) \
                .withColumn('margin_bottom', lit(0))

        pandf = df2.toPandas()

        labels = pandf['Aircraft'].drop_duplicates()
        msn = pandf['Mission'].drop_duplicates()

        fig, ax = plt.subplots(figsize=(20,10))

        #LEGEND COLOR CONFIGURATION
        NUM_COLORS = len(msn)
        cm = plt.get_cmap('tab20')
        ax.set_prop_cycle('color', [cm(1.*i/NUM_COLORS) for i in range(NUM_COLORS)])
        #END OF COLOR CONFIGURATION

        label_msn = []
        p_msn = []
        for m in msn:
                sub = pandf.loc[pandf['Mission'] == m]
                p1 = ax.bar(sub['Aircraft'], sub['NumMissions'], bottom=sub['margin_bottom'])

                label_msn.append(m)
                p_msn.append(p1)

                for a in labels:
                        if sub['Aircraft'].isin([a]).any():
                                pandf.loc[pandf['Aircraft'] == a, 'margin_bottom'] += sub.loc[sub['Aircraft'] == a, 'NumMissions'].item()

        
        ax.legend(p_msn, label_msn)
        ax.yaxis.set_major_formatter(mpt.EngFormatter())

        plt.xticks(rotation=25)
        plt.xlabel("Type of Aircraft")
        plt.ylabel("Number of missions")
        plt.title("Vietnam War Mission Type per Aircraft")
        plt.show()

def aircraft_per_bombings():

        df2 = df.select(col('VALID_AIRCRAFT_ROOT').alias('Aircraft'), col('MFUNC_DESC').alias('Mission'), col('NUMWEAPONSDELIVERED')) \
                .dropna() \
                .groupBy(col('Aircraft'), col('Mission')) \
                .agg(sum('NUMWEAPONSDELIVERED').alias('NumBombings')) \
                .filter(col('NumBombings') > 60000) \
                .orderBy(col('Aircraft')) \
                .withColumn('margin_bottom', lit(0))

        pandf = df2.toPandas()


        labels = pandf['Aircraft'].drop_duplicates()
        msn = pandf['Mission'].drop_duplicates()

        fig, ax = plt.subplots(figsize=(20,10))

        #LEGEND COLOR CONFIGURATION
        NUM_COLORS = len(msn)
        cm = plt.get_cmap('tab20')
        ax.set_prop_cycle('color', [cm(1.*i/NUM_COLORS) for i in range(NUM_COLORS)])
        #END OF COLOR CONFIGURATION

        label_msn = []
        p_msn = []
        for m in msn:
                sub = pandf.loc[pandf['Mission'] == m]
                p1 = ax.bar(sub['Aircraft'], sub['NumBombings'], bottom=sub['margin_bottom'])

                label_msn.append(m)
                p_msn.append(p1)

                for a in labels:
                        if sub['Aircraft'].isin([a]).any():
                                pandf.loc[pandf['Aircraft'] == a, 'margin_bottom'] += sub.loc[sub['Aircraft'] == a, 'NumBombings'].item()

        
        ax.legend(p_msn, label_msn)
        ax.yaxis.set_major_formatter(mpt.EngFormatter())

        plt.xticks(rotation=25)
        plt.xlabel("Type of Aircraft")
        plt.ylabel("Bombs dropped")
        plt.title("Vietnam War Bombings per Aircraft")
        plt.show()

def most_common_takeoff(): 
        df2 = df.select(col('TAKEOFFLOCATION').alias('Takeoff')) \
                .dropna() \
                .groupBy(col('Takeoff')) \
                .agg(count(lit(1)).alias('NumMissions')) \
                .filter(col('NumMissions') > 26000).filter(~(col('Takeoff').rlike('^WESTPAC.*$'))) \
                .orderBy(col('NumMissions').desc())

        pandf = df2.toPandas()
        print(pandf)
        fig, ax = plt.subplots(figsize=(16,9))

        ax.bar(pandf['Takeoff'], pandf['NumMissions'], color='royalblue')
        ax.legend()

        #avoid scientific notation on Y-axis
        #ax.ticklabel_format(style='plain', axis='y')
        ax.yaxis.set_major_formatter(mpt.EngFormatter())

        plt.xticks(rotation=20)
        plt.xlabel("Take-off location")
        plt.ylabel("Number of Missions")
        plt.title("Vietnam War Most Common Take-off Locations")
        plt.show()

def main():

        if len(sys.argv) < 2:
                total_bombings_bydate()
                total_missions_bydate()
                total_bombings_bycountry()
                total_missions_bycountry()
                most_attacked_countries()
                mission_types()
                most_attacked_locations_map()
                most_attacked_locations_map_bydate()
                aircraft_types()
                aircraft_per_type_of_mission()
                aircraft_per_bombings()
                most_common_takeoff()
                sys.exit(0)
        
        if int(sys.argv[1]) == 1:
                total_bombings_bydate()
        elif int(sys.argv[1]) == 2:
                total_missions_bydate()
        elif int(sys.argv[1]) == 3:
                total_bombings_bycountry()
        elif int(sys.argv[1]) == 4:
                total_missions_bycountry()
        elif int(sys.argv[1]) == 5:
                most_attacked_countries()
        elif int(sys.argv[1]) == 6:
                mission_types()
        elif int(sys.argv[1]) == 7:
                most_attacked_locations_map()
        elif int(sys.argv[1]) == 8:
                most_attacked_locations_map_bydate()
        elif int(sys.argv[1]) == 9:
                aircraft_types()
        elif int(sys.argv[1]) == 10:
                aircraft_per_type_of_mission()
        elif int(sys.argv[1]) == 11:
                aircraft_per_bombings()
        elif int(sys.argv[1]) == 12:
                most_common_takeoff()
        else:
                print("** ERROR: Wrong parameter **")
                sys.exit(1)
                        
       

if __name__ == "__main__":
        main()
