from datetime import date
import datetime
from time import time
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .serializers import CartItemSerializer
from .models import CartItem
from django.shortcuts import get_object_or_404
import pyspark
from pyspark.sql import SparkSession
from rest_framework.decorators import action
from rest_framework import viewsets
from django.core.files.base import ContentFile
from django.core.files.storage import FileSystemStorage
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pandas as pd
import pymongo
import io


fs = FileSystemStorage(location = '/home/vikas/Desktop/new_python_proj/shopping_cart/api_app')

class csvChunks(viewsets.ModelViewSet):
    queryset = CartItem.objects.all()
    serializer_class = CartItemSerializer
    
    @action(detail=False,methods=['POST'])
    def split_data(self,request):
        # return Response('1234')

        file = request.FILES["file"]
        
        
        type = request.data["type"]
        if(type=='chunk'):
            chunkcount = request.data["chunk"]

        if(type=='column'):
            column = request.data["column"]
        
        filename =file.name
        time = datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S')
        #return Response(file)
        #sc = SparkContext.getOrCreate()  # if using locally
        #sql_sc = SQLContext(sc)
        product_content = file.read()

        myclient = pymongo.MongoClient("mongodb://root:root@localhost:27017/?authMechanism=DEFAULT")
        mydb = myclient["assignment"]
        mycol = mydb["csv_data"]

        
        product_file_content =ContentFile(product_content)

        product_file_name = fs.save(
            "tmp/"+filename+time, product_file_content
        )
        product_tmp_file = fs.path(product_file_name)
        
        #Spark_Full = sc.emptyRDD()
        
        #   print(chunks)
        headers = list(pd.read_csv(product_tmp_file, nrows=0).columns)
        i = 0
        print(headers)
        if(type =='chunk'):
            chunks = pd.read_csv(product_tmp_file, chunksize=int(chunkcount))
            
        if(type=='column'):
            df = pd.read_csv(product_tmp_file, ',')
            #print(df)
            chunks = [rows for _, rows in df.groupby(column)]
        
        chunkcsvfilePath = 'data/'+type+'/'
        for i,chunk in enumerate(chunks):
            print(chunk)
            chunkfile = chunkcsvfilePath+'/chunk{}'.format(i+1)+datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S')+'.csv'
            chunk.to_csv(chunkfile, index=False)
            mydict = { "orignalfile": filename+time, "new_chunkfile": chunkfile, "type": type}
            x = mycol.insert_one(mydict)
        #     x = mycol.insert_one(mydict)
        # for chunk in chunks:
        #     print(chunk)
        #     pd.to_csv(chunkcsvfilePath+'file_name.csv', index=False)

        #     Spark_Full =  sc.parallelize(chunk.values.tolist())
        #     readfile = Spark_Full.toDF(headers)
        #     readfile.show()
        #     chunkcsvfile = chunkcsvfilePath +str(i)+  datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S')
        #     print(chunkcsvfile)
        #     i +=i
        #     readfile.write.csv(chunkcsvfile)
        #     mydict = { "orignalfile": filename+time, "new_chunkfile": chunkcsvfile }
        #     x = mycol.insert_one(mydict)
        #     return Response(234)
        
        #sc.stop()

        
        
        return Response("succesfully CSV Splited")
            


