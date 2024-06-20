import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def get_dataframes(pathToOutput):
    df_host_small = pd.read_parquet(f"{pathToOutput}/raw-output/0/seed=0/host.parquet")
    df_server_small = pd.read_parquet(f"{pathToOutput}/raw-output/0/seed=0/server.parquet")
    df_service_small = pd.read_parquet(f"{pathToOutput}/raw-output/0/seed=0/service.parquet")


    df_host_large = pd.read_parquet(f"{pathToOutput}/raw-output/1/seed=0/host.parquet")
    df_server_large = pd.read_parquet(f"{pathToOutput}/raw-output/1/seed=0/server.parquet")
    df_service_large = pd.read_parquet(f"{pathToOutput}/raw-output/1/seed=0/service.parquet")


    return df_host_small, df_server_small, df_service_small, df_host_large, df_server_large, df_service_large 

def getMeanUtilization(df_host):
    return df_host.cpu_utilization.mean()


def getTotalEnergyUsage(df_host, unit="joule"):
    if unit == "joule":
        return df_host.energy_usage.sum()
    if unit == "kWh":
        return df_host.energy_usage.sum() / 3_600_000
    
    raise ValueError(f"incorrect unit: {unit}. Allowed units are joule and kWh")

def getTotalRuntime(df_service):
    return pd.to_timedelta(df_service.timestamp.max() - df_service.timestamp.min(), unit="ms")

def get_dataframes(pathToOutput):
    df_host_small = pd.read_parquet(f"{pathToOutput}/raw-output/0/seed=0/host.parquet")
    df_server_small = pd.read_parquet(f"{pathToOutput}/raw-output/0/seed=0/server.parquet")
    df_service_small = pd.read_parquet(f"{pathToOutput}/raw-output/0/seed=0/service.parquet")


    df_host_large = pd.read_parquet(f"{pathToOutput}/raw-output/1/seed=0/host.parquet")
    df_server_large = pd.read_parquet(f"{pathToOutput}/raw-output/1/seed=0/server.parquet")
    df_service_large = pd.read_parquet(f"{pathToOutput}/raw-output/1/seed=0/service.parquet")


    return df_host_small, df_server_small, df_service_small, df_host_large, df_server_large, df_service_large 

def getMeanUtilization(df_host):
    return df_host.cpu_utilization.mean()


def getTotalEnergyUsage(df_host, unit="joule"):
    if unit == "joule":
        return df_host.energy_usage.sum()
    if unit == "kWh":
        return df_host.energy_usage.sum() / 3_600_000
    
    raise ValueError(f"incorrect unit: {unit}. Allowed units are joule and kWh")

def getTotalRuntime(df_service):
    return pd.to_timedelta(df_service.timestamp.max() - df_service.timestamp.min(), unit="ms")

def getMeanWaitTime(df_server):
    waitTimes = []

    for server_name, series in df_server.groupby("server_name"):
        waitTimes.append(series.host_id.isnull().sum()*300_000)

    return pd.to_timedelta(np.mean(waitTimes), unit="ms")


def plotService(df_service, column, label=""):
    plt.plot(df_service["timestamp"]/1000/60/60, df_service[column], label=label)
    plt.xlabel("Timestamp (h)")
    plt.ylabel(f"{column}")
    plt.legend()

def plotHosts(df_host, column, aggregation_method, label, window_size=1000):
    if aggregation_method not in ["mean", "sum"]:
        raise ValueError(f"incorrect aggregation method provided: {aggregation_method}, please pick on of [mean, sum]")

    df_agg = df_host.groupby("timestamp")[[column]].agg(aggregation_method)

    plt.plot(df_agg.index/1000/60/60, df_agg.rolling(window_size, min_periods=1).mean(), label=label)
    plt.xlabel("timestamp (h)")
    plt.ylabel(column)

    plt.legend()

def plotWaitTimesHist(df_server, label):
    waitTimes = []

    for server_name, series in df_server.groupby("server_name"):
        waitTimes.append(series.host_id.isnull().sum()*300_000/1000/60/60)

    plt.hist(waitTimes, alpha=0.8, label=label)
    plt.legend()
    plt.xlabel("wait time (h)")
    plt.ylabel("frequency")