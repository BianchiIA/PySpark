# -*- coding: utf-8 -*-
"""
Created on Sat May 14 21:40:05 2022

@author: Vinicius
"""

# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
spark = (SparkSession
         .builder
         .getOrCreate())
import numpy as np

nomes = ['Vinicius','Valéria','Larissa','Lucas']
notas = [int(v) for v in np.random.randint(1,11,4)]

# Criamos originalmente por uma lista de tuplas

data = [(nome,nota) for nome,nota in zip(nomes,notas)]
schema = 'nome STRING, nota INT'
df_custom = spark.createDataFrame(data=data,
                                  schema = schema)
df_custom.show()