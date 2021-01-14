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
import os
import argparse

def total_bombings_bydate():
        df2 = df.select(last_day("MSNDATE").alias("Date"), col("COUNTRYFLYINGMISSION").alias("Country"), col("NUMWEAPONSDELIVERED")) \
                .dropna() \
                .groupBy(col("Date"), col("Country")) \
                .agg(sum("NUMWEAPONSDELIVERED").alias("NumWeapons")) \
                .orderBy("Date")

        pandf = df2.toPandas()

        fig, ax = plt.subplots(figsize=(19,10))

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
        plt.savefig(output_dir + "total_bombings_bydate.png")
        plt.close(fig)

def total_missions_bydate(): 
        df2 = df.select(last_day("MSNDATE").alias("Date"), col("COUNTRYFLYINGMISSION").alias("Country")) \
                .dropna() \
                .groupBy(col("Date"), col("Country")) \
                .count() \
                .orderBy("Date")

        pandf = df2.toPandas()

        fig, ax = plt.subplots(figsize=(19,10))

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
        plt.savefig(output_dir + "total_missions_bydate.png")
        plt.close(fig)

def total_bombings_bycountry():
        df2 = df.select(col("COUNTRYFLYINGMISSION").alias("Country"), col("NUMWEAPONSDELIVERED")) \
                .dropna(subset=["Country"]) \
                .groupBy(col("Country")) \
                .agg(sum("NUMWEAPONSDELIVERED").alias("NumWeapons")) \
                .orderBy(col('NumWeapons').desc())

        pandf = df2.toPandas()

        fig, ax = plt.subplots(figsize=(10,9))

        ax.bar(pandf['Country'], pandf['NumWeapons'], color='royalblue')
        ax.legend()

        #avoid scientific notation on Y-axis
        ax.yaxis.set_major_formatter(mpt.EngFormatter())

        # rotate and align the tick labels so they look better
        plt.xticks(rotation=10)
        plt.xlabel("Countries")
        plt.ylabel("Bombs dropped")
        plt.title("Vietnam War Bombings (Totals by Country)")
        plt.savefig(output_dir + "total_bombings_bycountry.png")
        plt.close(fig)

def total_missions_bycountry(): 
        df2 = df.select(col("COUNTRYFLYINGMISSION").alias("Country")) \
                .dropna() \
                .groupBy(col("Country")) \
                .count() \
                .orderBy(col('count').desc())

        pandf = df2.toPandas()

        fig, ax = plt.subplots(figsize=(9,9))

        ax.bar(pandf['Country'], pandf['count'])
        ax.legend()

        #avoid scientific notation on Y-axis
        ax.yaxis.set_major_formatter(mpt.EngFormatter())

        plt.xticks(rotation=10)
        plt.xlabel("Countries")
        plt.ylabel("Number of Missions")
        plt.title("Vietnam War Missions (Totals by Country)")
        plt.savefig(output_dir + "total_missions_bycountry.png")
        plt.close(fig)

def most_attacked_countries(): 
        df2 = df.select(col('TGTCOUNTRY').alias('TargetCountry'), col('NUMWEAPONSDELIVERED').alias('NumWeapons')) \
                .dropna() \
                .groupBy(col('TargetCountry')) \
                .agg(sum('NumWeapons').alias('TotalWeaponsDropped')) \
                .filter(col('TotalWeaponsDropped') > 0).filter(col('TargetCountry') != 'UNKNOWN') \
                .orderBy(col('TotalWeaponsDropped').desc())

        pandf = df2.toPandas()

        fig, ax = plt.subplots(figsize=(10,9))

        ax.bar(pandf['TargetCountry'], pandf['TotalWeaponsDropped'], color='royalblue')
        ax.legend()

        #avoid scientific notation on Y-axis
        #ax.ticklabel_format(style='plain', axis='y')
        ax.yaxis.set_major_formatter(mpt.EngFormatter())

        plt.xticks(rotation=10)
        plt.xlabel("Attacked Countries")
        plt.ylabel("Number of Bombs dropped")
        plt.title("Vietnam War Most Affected Countries")
        plt.savefig(output_dir + "most_attacked_countries.png")
        plt.close(fig)

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

        fig, ax = plt.subplots(figsize=(19,10))

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
        plt.savefig(output_dir + "mission_types.png")
        plt.close(fig)

def most_attacked_locations_map():
        df2 = df.select(round(col('TGTLATDD_DDD_WGS84'), 4).alias('Latitude'), round(col('TGTLONDDD_DDD_WGS84'), 4).alias('Longitude'), col('NUMWEAPONSDELIVERED').alias('Bombs dropped')) \
                .filter(col('Bombs dropped') > 0) \
                .dropna() \
                .groupBy('Latitude', 'Longitude') \
                .agg(sum('Bombs dropped').alias('Bombs dropped'), count(lit(1)).alias('Number of attacks')) \
                .filter(col('Bombs dropped') >= 150)

        pandf = df2.toPandas()

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

        fig.write_html(output_dir + "total_attacked_locations.html")

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

        fig.write_html(output_dir + 'attacked_locations_bydate.html')

def aircraft_types():
        df2 = df.select(col('VALID_AIRCRAFT_ROOT').alias('Aircraft')) \
                .dropna() \
                .groupBy(col('Aircraft')) \
                .count() \
                .orderBy(col('count').desc()) \
                .filter(col('count') > 7000)

        pandf = df2.toPandas()

        fig, ax = plt.subplots(figsize=(20,10))

        ax.bar(pandf['Aircraft'], pandf['count'], color='royalblue')

        ax.yaxis.set_major_formatter(mpt.EngFormatter())

        plt.xticks(rotation=25)
        plt.xlabel("Type of Aircraft")
        plt.ylabel("Number of missions")
        plt.title("Vietnam War Most used Aircrafts")
        plt.savefig(output_dir + "aircraft_types.png")
        plt.close(fig)

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
        plt.savefig(output_dir + "aircraft_per_type_of_mission.png")
        plt.close(fig)

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
        plt.savefig(output_dir + "aircraft_per_bombings.png")
        plt.close(fig)

def most_common_takeoff(): 
        df2 = df.select(col('TAKEOFFLOCATION').alias('Takeoff')) \
                .dropna() \
                .groupBy(col('Takeoff')) \
                .agg(count(lit(1)).alias('NumMissions')) \
                .filter(col('NumMissions') > 26000).filter(~(col('Takeoff').rlike('^WESTPAC.*$'))) \
                .orderBy(col('NumMissions').desc())

        pandf = df2.toPandas()

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
        plt.savefig(output_dir + "most_common_takeoff.png")
        plt.close(fig)

if __name__ == "__main__":
        parser = argparse.ArgumentParser(
                description="Analyzes Vietnam bombing data using Spark",
                formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument("-f",
                dest="filename",
                action="store",
                default="THOR_Vietnam_Bombing_Operations.csv",
                help="input data (default: %(default)s)")
        parser.add_argument("-o",
                metavar="FOLDER",
                dest="output",
                action="store",
                default="output",
                help="output folder (default: %(default)s)")
        parser.add_argument("analyses",
                metavar="N",
                nargs="*",
                default=range(12),
                help="index of analysis that will be run (defaults to all)\n"
                     "    0: total bombings by date\n"
                     "    1: total missions by date\n"
                     "    2: total bombings by country\n"
                     "    3: total missions by country\n"
                     "    4: most attacked countries\n"
                     "    5: mission types\n"
                     "    6: most attacked locations map\n"
                     "    7: most attacked locations map by date\n"
                     "    8: aircraft types\n"
                     "    9: aircraft per type of mission\n"
                     "    10: aircraft per bombings\n"
                     "    11: most common takeoff")
        args = parser.parse_args()

        list_colors = ['r', 'b', 'k', 'g', 'y', 'm', 'c']
        output_dir = args.output + "/"
        analyses = [
                total_bombings_bydate,
                total_missions_bydate,
                total_bombings_bycountry,
                total_missions_bycountry,
                most_attacked_countries,
                mission_types,
                most_attacked_locations_map,
                most_attacked_locations_map_bydate,
                aircraft_types,
                aircraft_per_type_of_mission,
                aircraft_per_bombings,
                most_common_takeoff
        ]

        try:
                os.mkdir(output_dir)
        except (FileExistsError, OSError):
                pass

        conf = SparkConf().setMaster("local[*]").setAppName("VietAnalysis")
        sc = SparkContext(conf = conf)
        spark = SparkSession(sc)

        df = spark.read.csv(args.filename, header=True, inferSchema=True)

        for i in args.analyses:
                try:
                        analyses[int(i)]()
                except IndexError:
                        print("ERROR: Invalid analysis index: " + i)
                        sys.exit(1)
                except ValueError:
                        print("ERROR: Index must be a number: " + i)
                        sys.exit(1)

