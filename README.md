# Assignment-3

## Live application Links
[![codelabs](https://img.shields.io/badge/codelabs-4285F4?style=for-the-badge&logo=codelabs&logoColor=white)](https://codelabs-preview.appspot.com/?file_id=10r1eR1q76BIH4aWlLfZp-t_VrQO7br07bXHtnsOxxm0#0)


- video : https://youtu.be/0l1MtgwPTwg

## Problem Statement 
The task is to build an incremental data pipeline in Snowflake using Snowpark for processing CO2 emissions data from Mauna Loa Observatory. The pipeline will ingest daily data from an external source, transform and harmonize it using Snowpark Python, and store it in Snowflake. Key components include using Snowflake Tasks, Streams, UDFs, and a stored procedure for automation and incremental updates. The final deliverables will include a fully functional pipeline, GitHub repository with code and tests, and comprehensive documentation. 
## Project Goals
1.  Use Snowpark Python to create an incremental pipeline in Snowflake that will ingest and transform CO2 emissions data.
   
2.  Use Snowflake Streams, Tasks, and a stored procedure for incremental updates to automate daily data processing and changes.

3.  Make analytics tables for daily and weekly performance measures and put data harmonisation into practice.

4.  Use Jinja templates for environment management, set up GitHub Actions for continuous deployment, and apply unit tests for validation.


## Technologies Used
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/)
[![Amazon AWS](https://img.shields.io/badge/Amazon_AWS-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com/)
[![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)](https://www.python.org/)
[![Snowflake](https://img.shields.io/badge/snowflake-%234285F4?style=for-the-badge&logo=snowflake&link=https%3A%2F%2Fwww.snowflake.com%2Fen%2F%3F_ga%3D2.41504805.669293969.1706151075-1146686108.1701841103%26_gac%3D1.160808527.1706151104.Cj0KCQiAh8OtBhCQARIsAIkWb68j5NxT6lqmHVbaGdzQYNSz7U0cfRCs-STjxZtgPcZEV-2Vs2-j8HMaAqPsEALw_wcB&logoColor=white)
](https://www.snowflake.com/en/?_ga=2.41504805.669293969.1706151075-1146686108.1701841103&_gac=1.160808527.1706151104.Cj0KCQiAh8OtBhCQARIsAIkWb68j5NxT6lqmHVbaGdzQYNSz7U0cfRCs-STjxZtgPcZEV-2Vs2-j8HMaAqPsEALw_wcB)

## Pre requisites
1. Python Knowledge
2. Snowflake Account
3. AWS S3 bucket
4. Snowpark
5. Git Repositories
   
## 📂 Project Structure
```
├── .github
   └── workflow
    └── build_and_deploy.yml
├── notebooks
    └── daily_updates.ipynb  
├── scripts
   └── data_ingestion.sql
   └── deploy_notebooks.sql
   └── deploy_task_dag.py
   └── environment.yml
   └── teardown.sql 
├── AiUseDisclosure
├── NEW_SCRATCH(2).ipynb
├── README.md
├── deploy_snowpark_apps.py
├── environment.yml
├── requirements.txt
├── script.py


```




## Project run outline

CodeLab -(https://codelabs-preview.appspot.com/?file_id=10r1eR1q76BIH4aWlLfZp-t_VrQO7br07bXHtnsOxxm0#0)

## References
Snowflake Docs



  
  Name | Contribution %|
  --- |--- |
Pranjal Mahajan(002375449)  | 33.33% | 
 Srushti Patil (002345025)   | 33.33% | 
 Ram Putcha (002304724) | 33.33% |
