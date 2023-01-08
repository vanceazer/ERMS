import requests
import pandas as pd
import os

rootPath = 'C:/Users/akand/Downloads/loadertest/Governorship election - 2022-07-16 - OSUN'

lgaList = []

url = 'http://localhost:8888/api/v1/IrevResult/push-result-data-model'
puCodeUrl = 'https://core-erms.herokuapp.com/api/v1/PoolingUnit/code'
lgaurl = 'https://core-erms.herokuapp.com/api/v1/lga/'


# define function to get polling unit details from code
def ProcessData(code):
    body = {
        "poolingUnitCode": code
    }

    puResponse = requests.post(puCodeUrl, json=body)
    statusCode = puResponse.status_code
    print(statusCode)
    if statusCode == 200:
        print('Processing Successful')
        # print(puResponse.json())
        puDf = pd.DataFrame(puResponse.json()['data'])

        for index, row in puDf.iterrows():
            puName = row['name']
            # print(puName)
            wardData = row['ward']
            # print(wardData)
            puWardName = wardData['name']
            # print(puWardName)
            puLgaId = wardData['lgaId']
            # print(puLgaId)


            # get pu lga details from lgacode
            lgaIdUrl = f'{lgaurl}{puLgaId}'
            # print(lgaIdUrl)
            lgaResponse = requests.get(lgaIdUrl)
            # print(lgaResponse.json()['data'])
            lgaDataJson = lgaResponse.json()['data']
            puLgaName = lgaDataJson['name']
            # print(puLgaName)
            # puLgaDf = pd.DataFrame(lgaResponse.json()['data'])
            # print(puLgaDf)
            print("All Polling Unit data is")
            print('polling unit name is', puName)
            print('ward name is', puWardName)
            print('lga name is', puLgaName)


            # Processing Retrieved details to upload resultImage
            body = {
                "Data.Election": "Governorship election - 2022-07-16 - OSUN",
                "Data.PoolingUnit": puName,
                "Data.PoolingUnitCode": pu_Code,
                # "Data.Location"
                "Data.Address": "Unknown",
                "Data.Latitude": "Unknown",
                "Data.longitude": "Unknown",
                "Data.Ward": puWardName,
                "Data.LocalGovernment": puLgaName,
                "Data.State": "OSUN",
                "Data.GeoZone": "South South",
                "Data.zone": "South",
                "Data.presidingOfficer.id": "1",
                "Data.presidingOfficer.name": "System Autogenerator",
                "Data.votersOnRegister": 0,
                "Data.accreditedVoters": 0,
                "Data.ballotPapersIssuedToPoolingUnit": 0,
                "Data.unusedBallotPapers": 0,
                "Data.rejectedBallot": 0,
                "Data.totalValidVotes": 0,
                "Data.totalUsedBallotPapers": 0,
                "Data.status": "Approved",
                "Data.approvedBy": "System Autogenerator"
            }

            print('Attempting to push')
            print(body)
            file_payload = {'upload': open(pdf_path, 'rb')}
            print(file_payload)

            data_upload = requests.post(url, data=body, files=file_payload)
            # data_upload = requests.post(url, data=body)
            response = data_upload.status_code
            print(response)
            # print(data_upload.request.body)

            if response == 202:
                print('Data Upload Successful')
                print(data_upload.json())

            else:
                print(response)
                print(data_upload.json())
                # print('Shit went wrong')








    # else:
    #     print('Processing Failed')
    #     print(puResponse.json())







# looping through the path to get the lga folders
for lgaFolder in os.listdir(rootPath):
    # print(lgaFolder)
    lgaList.append(lgaFolder)

df_lga = pd.DataFrame(lgaList, columns=['lgaName'])
# print(df_lga)


for i, lgaRows in df_lga.iterrows():
    if i >= 0:
        print('LGA INDEX = ', i)
        print('Processing LGA', lgaRows['lgaName'])
        # establish lga path
        lgaPath = os.path.join(rootPath, lgaRows['lgaName']).replace("\\", "/")
        # print(lgaPath)

        wardList = []
#         looping through the path to get ward folders
        for wardFolder in os.listdir(lgaPath):
            # print(wardFolder)
            wardList.append(wardFolder)

        df_ward = pd.DataFrame(wardList, columns=['wardName'])
        # print(df_ward)

        for i_2, wardRows in df_ward.iterrows():
            if i_2 >= 0:
                print('WARD INDEX = ', i_2)
                print('Processing Ward', wardRows['wardName'])
#                 establish ward path

                wardPath = os.path.join(lgaPath, wardRows['wardName']).replace("\\", "/")
                # print(wardPath)

                puList = []

                for puFolder in os.listdir(wardPath):
                    # print(puFolder)
                    puList.append(puFolder)

                df_pu = pd.DataFrame(puList, columns=['puCodeFolder'])
                # print(df_pu)


                for i_3, puRows in df_pu.iterrows():
                    if i_3 >= 0:
                        print('PU INDEX = ', i_3)
                        print('Processing PU Code', puRows['puCodeFolder'])
                        # puCodeee = puRows['puCodeFolder']

                        #   establish pu path

                        puPath = os.path.join(wardPath, puRows['puCodeFolder']).replace("\\", "/")
                        print(puPath)
                        pdf_name = os.listdir(puPath)[0]
                        print(pdf_name)
                        # establish pdf result path
                        pdf_path = os.path.join(puPath, pdf_name).replace("\\", "/")
                        print(pdf_path)
                        puCode = puRows['puCodeFolder'][10:]
                        print(puCode)
                        pu_Code = 'PU-'+ puCode
                        print(pu_Code)

                        # getting polling unit details
                        ProcessData(puCode)
                        print('-------------------------------------------------------------------------------------')





print('Processing Completed')








