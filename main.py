import boto3
import json
import logging
import re
import os
import datetime
from datetime import datetime
import sys
import pandas as pd
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)
codepipeline = boto3.client('codepipeline')
pipelines_dict=codepipeline.list_pipelines()['pipelines']
HOOK_URL = "https://hooks.slack.com/services/%s" %  (os.environ['GENERAL_WEBHOOK_SECRET'])
pipes_url="https://eu-west-1.console.aws.amazon.com/codesuite/codepipeline/pipelines/"

def ParcePipelines(data, param, param2,param3, param4, param5,param6):
    VavidPipeLinesContext=[]
    EmptyPipeLinesContext=[]  
    VavidPipeLinesNames=[]
    EmptyPipeLinesNames=[]
    allPipelinesContext=[data[i] for i in range(len(data)) if data[i][param] == data[i][param]]

    for i in range(len(data)):
        if len(allPipelinesContext[i][param2][0].keys()) <= 3:
            EmptyPipeLinesNames.append(allPipelinesContext[i][param])
        elif len(allPipelinesContext[i][param2][0].keys()) > 3:
            VavidPipeLinesNames.append(allPipelinesContext[i][param])

    for i in range(len(data)):
        if len(allPipelinesContext[i][param2][0].keys()) <= 3:
            EmptyPipeLinesContext.append(allPipelinesContext[i])
        elif len(allPipelinesContext[i][param2][0].keys()) > 3:
            VavidPipeLinesContext.append(allPipelinesContext[i])
    pipelineExecutionId=[VavidPipeLinesContext[i][param2][0][param3][param5] for i in range(len(VavidPipeLinesContext))]
    ConvertedToHours=[[round((datetime.now() - VavidPipeLinesContext[z][param2][0][param4][0][param3][param6].replace(tzinfo=None)).total_seconds()/3600), "Hours ago!"]  for z in range(len(VavidPipeLinesContext))]
    return VavidPipeLinesNames, pipelineExecutionId, ConvertedToHours, allPipelinesContext, EmptyPipeLinesNames, VavidPipeLinesContext, EmptyPipeLinesContext
  
def get_pipeline_execution(data):
    status_checking=[[codepipeline.get_pipeline_execution(pipelineName=[i for i in data[0]][z], pipelineExecutionId=[i for i in data[1]][z])] for z in range(len(data[0]))]                               
    return status_checking    
    
def lambda_handler(event, context):
    pipelines_name_list=[]
    for pipeline in pipelines_dict:
        pipelines_name_list.append(pipeline['name'])
    PipelineExecutionStage=[]
    for pipeline in pipelines_dict:
        PipelineExecutionStage.append(codepipeline.get_pipeline_state(name=pipeline['name']))
        datas=ParcePipelines(PipelineExecutionStage,'pipelineName','stageStates','latestExecution','actionStates','pipelineExecutionId','lastStatusChange')

    pipeline_executionStage=get_pipeline_execution(datas)
    preReport=[[pipeline_executionStage[i][0]['pipelineExecution']['pipelineName'], pipeline_executionStage[i][0]['pipelineExecution']['status']] for i in range(len(pipeline_executionStage)) if pipeline_executionStage[i][0]['pipelineExecution']['status'] != 'Succeeded']

    dfWithOUTStatusExecution=pd.DataFrame(ParcePipelines(PipelineExecutionStage,'pipelineName','stageStates','latestExecution','actionStates','pipelineExecutionId','lastStatusChange')).T.rename(columns={0:'pipelineName',1:'executionId',2:'Last executed (Hours Ago!)'})
    dfWithStatusExecution=pd.DataFrame(preReport, columns=['pipelineName','Most recent execution'])
    
    def mergingDataFrames(data1,data2):
        for line1 in data1['pipelineName']:
            for line2 in data2['pipelineName']:
                if line1 ==line2:
                    result=data1.merge(data2)


        return result
    table_df=mergingDataFrames(dfWithStatusExecution, dfWithOUTStatusExecution)
    table_df1=table_df.drop('executionId', axis=1).drop(3, axis=1).drop(4, axis=1).drop(5, axis=1).drop(6, axis=1).values.tolist()
    results=[]
    for line in table_df1:
        if line[2][0] > 6:
            results.append(("<{0}{1}/view?region=eu-west-1|{1}> %s " % "{2}").format(pipes_url, line[0], line[1::]))

    nl = '\n'
    text = f"<!channel> {nl}{nl.join(results)}"
    slack_message = {'text': text, 'Attachment': "testing"}
    #req = requests.post(HOOK_URL, json.dumps(slack_message))

