#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 15 19:21:02 2022

@author: vinicius
"""

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

spark = (SparkSession.builder.getOrCreate())



path ='/media/vinicius/Storage/Stronger/Repositorio_Dados_Brutros/Empresa/Empresa/teste'

df =  spark.read.csv(path)