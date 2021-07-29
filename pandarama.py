import uuid
import pandas as pd
import datetime

import boto3
from botocore.exceptions import ClientError

class Pandamonium():
    def __init__(self,sourcefile):
        self.sourcefile = sourcefile
        self.df = None

    def load_dataframe(self):
        start_time = datetime.datetime.now()
        print(f"Started method load_dataframe() / time: {start_time}")
        self.df = pd.read_csv(self.sourcefile,sep='|')
        print(f"Loaded DF / time: {datetime.datetime.now()}")

        # print(self.df)
        self.df['rowid'] = self.df.index.to_series().map(lambda x: uuid.uuid4())
        end_time = datetime.datetime.now()
        print(f"Added uuid for each row in DF / time: {end_time}")
        print(f"Seconds taken to complete uuid updates to DF / time: {abs((end_time - start_time).seconds)}")
        self.df.to_csv("C:\TestData\CSV-PIPED\TestOutput.CSV",sep='|',index=False)
        print(f"Loaded DF / time:{datetime.datetime.now()}")






