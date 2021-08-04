# Databricks notebook source
# MAGIC %sh 
# MAGIC pip install bs4
# MAGIC pip install --upgrade pip
# MAGIC python -m bs4.downloader all

# COMMAND ----------

import urllib
from bs4 import BeautifulSoup

def convertUrlToText(s):# converts a url,s, to raw text from website
  #user agent and headers so no HTTPErrors are thrown due to not being permited to access webpage. 
  user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
  headers={'User-Agent':user_agent,}
  #s = s.strip()
  builtRequest = urllib.request.Request(s,None,headers)
  html = (urllib.request.urlopen(builtRequest).read())
  soup = BeautifulSoup(html, features = "html.parser")#https://stackoverflow.com/questions/328356/extracting-text-from-html-file-using-python
  for script in soup(["script", "style"]):#removing script and style items from soup
    script.extract()  
  text = s+'\n'+soup.get_text()#adding url to begging
  #text = soup.get_text()
  return text

# COMMAND ----------


#creates a dictionary of lines num where the value of each line coresponds to another other dictionary that represents each word in the line with a offset number as the key. e.g. line: hello word = 1:{0:"hello",1:"world"}, where the first 1 represents the line number
def lineNumPairs(lines):#takes a lines of text as input
  lineDict = {}#declaring dictionary to store line num with a dictionary as a key to store the words in the dict
  for i in range(len(lines)):#loop through lines
    line = lines[i].split(" ")#for each line split and create an array of words which are in line
    #wordDict = {}#dict for storing word and offset pair
    word_num_list = line
    for j in range(len(line)):#loop through words in line and create a offset and word pair
      #wordDict.update({str(j):line[j]})
      word_num_list[j] =(str(j),line[j]) 
    #lineDict.update({lines[0] + " line:" + str(i):wordDict})
    lineDict.update({str(i):word_num_list})
  return lineDict #return dict whcih consists of line num as key, with value as another dict which coresponds to the words in the line and offset value!
  

# COMMAND ----------

dbutils.fs.ls("/FileStore/shared_uploads/13bourned@gmail.com/")
#creates rdd from text files
webpagesURLS = sc.textFile("/FileStore/shared_uploads/13bourned@gmail.com/WikipediaUrls.txt")

# COMMAND ----------

webpagesText = webpagesURLS.map(convertUrlToText)# rdd for storing exstracted text
list = webpagesText.collect()
print(list)

# COMMAND ----------

#splitting text into lines 
webpagesLines = webpagesText.map(lambda x: x.split("\n"))
lineNumPairsRDD = webpagesLines.map(lineNumPairs)#couldnt create a lambda exspression to do this 
list = lineNumPairsRDD.collect()
print(list)

# COMMAND ----------

#converting to a data frame so can query for a regex
#this was my apithiny with lambda exspressions. finally have the hang of it
flat = lineNumPairsRDD.flatMap(lambda x: [(x.get('0')[0][1],)+i for i in x.items()])#lambda exspression that converts rdd of line num pairs in to a triple with the coresponding url. This is done as a flatmap so that all triples can be put into one data frame so that when quried for a word it also returns to the corosponding line number and webpage. If was not a flat map a data frame would have to be created for each webpage which isnt that efficent.
list = flat.collect()
print(list)

#grep = flat.filter(lambda x: x[2][y][1] == )#where y can be anythong
#grep = df.select([c for c in df.columns]).where(df.col("Words"))


# COMMAND ----------

#applying filter: tuples, that corespond to a line, with out a word that coresponds to a regex are removed 
import re
regex = re.compile('\W*(user)\W*')
#grep = flat.filter(lambda x: i for i in x[2] if regex.match(i[1]))
#grep = flat.filter(lambda x:(i for i in x[2] if regex.match(x[2][i][1])))
grep =flat.filter(lambda x:[i for i in (x[2]) if regex.match(i[1])])
list = grep.collect()
print(list)

# COMMAND ----------

from pyspark.sql.functions import lit, col, UserDefinedFunction
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
import re
#now for structureing and querying the data. Inorder to do this will use a
columns = ["URL","LineNum","Line"]#columns for the dataframe
df = grep.toDF(columns) #creating a dataframe where columns represents the schema

def filterWords(col):
  matched = []
  for i in col:
    if regex.match(i[1]):
      matched.append(i)
  return matched
             
udf = UserDefinedFunction(filterWords, ArrayType(StructType([StructField("offset",StringType(),False), StructField("word",StringType(),False)])))

df = df.withColumn("Words", udf(df.Line))

df.show()

df.select("URL","LineNum","Words").display()

# COMMAND ----------

import unittest

class TestProblemTwo(unittest.TestCase):
  def test_lineNumPairs(self):
    list = ["www.example.com", "hello", "I am", "a", "KEY Value pair"]
    #self.assertEqual(lineNumPairs(list),{0:"www.example.com", 1:"hello", 2:"I am", 3:"a", 4:"KEY"})
    #self.assertEqual(lineNumPairs(list),{ "0":{"0":"www.example.com"}, "1":{"0":"hello"}, "2":{"0":"I", "1":"am"},"3":{"0":"a"},"4":{"0":"KEY","1":"Value","2":"pair"}})
    self.assertEqual(lineNumPairs(list),{ "0":[("0","www.example.com")], "1":[("0","hello")], "2":[("0","I"), ("1","am")],"3":[("0","a")],"4":[("0","KEY"),("1","Value"),("2","pair")]})
    #self.assertEqual(lineNumPairs(list),{ "www.example.com line:0":{"0":"www.example.com"}, "www.example.com line:1":{"0":"hello"}, "www.example.com line:2":{"0":"I", "1":"am"},"www.example.com line:3":{"0":"a"},"www.example.com line:4":{"0":"KEY","1":"Value","2":"pair"}})
    
  def test_fiterWords(self):
    col= [('0', 'Current'), ('1', 'usage'), ('2', 'of'), ('3', 'user'),('4', 'term'), ('5', 'big'), ('6', 'data'), ('7', 'tends'), ('8', 'to'), ('9', 'user'), ('10', 'to'), ('11', 'the'), ('12', 'use'), ('13', 'of')]
    self.assertEqual(filterWords(col),[('3', 'user'),('9', 'user')])
  
  def test_convertUrlToText(self):
    # hard to test a web scraping funtion so i am producing a mock an testing for exspected outcomes
  # inspiration given by https://www.tutorialspoint.com/python_web_scraping/python_web_scraping_testing_with_scrapers.htm
    url = "https://en.wikipedia.org/wiki/Node"
    user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
    headers={'User-Agent':user_agent,}
    builtRequest = urllib.request.Request(url,None,headers)
    html = (urllib.request.urlopen(builtRequest).read())
    soup = BeautifulSoup(html, features = "html.parser")
    
    #test to see if text can be retreived
    self.assertEqual(soup.find('h1').get_text(),"Node")
    
    for script in soup(["script", "style"]):#removing script and style items from soup
      script.extract()  
    
    #test to see if all style and script elements are removed
    i = 0
    for script in soup(["script", "style"]):
      i += 1
    self.assertEqual(i,0)
      
    #test to see see if text can be exstracted still
    self.assertIsNotNone(soup.get_text())
    
    #ensuring urls is the first line in the text
    textTest = url+'\n'+soup.get_text()
    textTest = textTest.split("\n")
    
    self.assertEqual(textTest[0],url)
    
if __name__ == "__main__":
    #unittest.main()
    #print("All Tests Passed")
    #haribaskar - https://forums.databricks.com/questions/14435/how-do-i-run-unit-tests-in-a-notebook-via-nosetest.html
    suite = unittest.TestLoader().loadTestsFromTestCase(TestProblemTwo)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
    #print("All Tests Passed")
