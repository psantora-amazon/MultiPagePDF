import boto3
from time import time, sleep
import pandas as pd


class MultiPagePDF:

    def __init__(self, s3BucketName, objectName):
        self.client = boto3.client('textract')
        self.bucket = s3BucketName
        self.file = objectName
        self.jobID = None
        self.nextToken = None
        self.jobStatus = None
        self.pages = list()
        self.FINISHED = False
        self.start()

    def make_args(self):
        args = {
            "JobId": self.jobID
        }
        if self.nextToken:
            args['NextToken'] = self.nextToken
            return args
        else:
            return args

    def start(self):
        res = self.client.start_document_text_detection(
            DocumentLocation={
                'S3Object': {
                    'Bucket': self.bucket,
                    'Name': self.file
                }
            })
        self.jobID = res["JobId"]

        print(f'Started job with id: {self.jobID}')

        return self.jobID

    def get_update(self):
        res = self.client.get_document_text_detection(**self.make_args())
        self.nextToken = res.get('NextToken', None)
        self.jobStatus = res.get('JobStatus', None)
        if self.jobStatus != "SUCCEEDED":
            sleep(5)
            print(f'Job status: {self.jobStatus}')
        elif self.nextToken:
            self.pages.append(res)
            print(f'Result page received: {len(self.pages)}')
        else:
            self.FINISHED = True
            return

    def get_results(self):
        start_time = time()
        while not self.FINISHED:
            print("--- %s seconds ---" % (time() - start_time))
            self.get_update()

        return self.pages

    def build_dataframe(self):
        row_list = []
        for page in self.pages:
            for item in page.get('Blocks'):
                if item.get("BlockType") == "LINE":
                    dict1 = {
                        'Page Num': item.get('Page'),
                        'Line Num': len(row_list),
                        'Line': item.get('Text')
                    }
                    row_list.append(dict1)

        df = pd.DataFrame(row_list)
        return df

    def lines_to_csv(self, fileName=None):
        if not fileName:
            fileName = self.file
        df = self.build_dataframe()
        return df.to_csv(
            path_or_buf=fileName,
            index=False
        )

if __name__ == '__main__':

    # Document
    bucketName = "" # Enter your s3. bucket
    documentName = "" # Enter name of your file
    pdf_textract = MultiPagePDF(bucketName, documentName)
    pages = pdf_textract.get_results()
    pdf_dataframe = pdf_textract.build_dataframe()


