# Databricks notebook source
# define a widget with a default value
dbutils.widgets.text("my_param", "default_value")

# get the value
param_value = dbutils.widgets.get("my_param")

print(f"My parameter is: {param_value}")