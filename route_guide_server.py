from cgitb import text
import asyncio
from sqlalchemy import create_engine
import logging
import numpy as np
from sqlalchemy import text
import pandas as pd
import grpc
from content_base import ContentBase
import rs_pb2
import rs_pb2_grpc
from dotenv import load_dotenv
load_dotenv()

class RecommendationServicer(rs_pb2_grpc.RecommendationServicer):
    def __init__(self):
        self.server = '127.0.0.1'
        self.user = 'postgres'
        self.password = '123456'
        self.port = '5432'
        self.database = 'ApiManagement_Module'
        self.databaseIS = 'ApiManagement_Main'

        yhat, users, data = InitDb(self)
        print('yhat', yhat)
        self.yhat = yhat
        self.users = users
        self.data = data

    def TrackChange(self, request, context):
        print('track change')
        try:
          yhat, users, data = InitDb(self)
          self.yhat = yhat
          self.users = users
          self.data = data
          return rs_pb2.Check(message='success')
        except:
          return rs_pb2.Check(message='failed')

    def GetItemRecommended(self, request, context):
        indexUserId = self.get_Index_user(request.id)
        itemIdsRated = self.yhat[:, indexUserId]
        output = np.asarray([idx for idx, element in enumerate(itemIdsRated) if (element > 0)])
        print('output 1', output)

        if output.size == 0:
            # Get Most popular
            itemIds = self.GetMostPularItem()
            return rs_pb2.ItemResponse(itemIds=itemIds)
        else:
            # get rated item
            itemIds = self.data[output, 0]
            print('itemIdsRated', itemIdsRated)
            print('output', output)
            #return index of a sorted list
            # indexItemSortedIds = sorted(range(len(itemIds)), key=lambda k: itemIds[k], reverse=True)
            indexItemSortedIds = sorted(range(len(itemIdsRated)), key=lambda k: itemIdsRated[k], reverse=True)
            print('item sorted', indexItemSortedIds)
            return rs_pb2.ItemResponse(itemIds=self.data[:,0][indexItemSortedIds]) #return sorted List ids of item by uuid

    def GetMostPularItem(self):
        sumArr = np.asarray(list(map((lambda  x: sum(x)), self.yhat)))
        # return index of a sorted list
        indexItemSortedIds = sorted(range(len(sumArr)), key=lambda k : sumArr[k], reverse=True)
        return self.data[:,0][indexItemSortedIds] #return sorted List ids of item by uuid

    def get_Index_user(self, userId):
        print("self::get_Index_user",self.users)
        ids = np.where(self.users == userId)[0][0]
        return ids

# item: prop in propList [IdProp, CateName]
# l_tags: categories [Id, Name] - VehicleType
# Check trong các categories, nếu chứa thuộc tính item đang xét thì đánh 1, ngược lại đánh 0
def mapData(item, l_tags):
      i_map = list(map((lambda x:  0 if x['Name'] not in item[1] else 1), l_tags))
      i_map.insert(0, item[0])
      return np.asarray(i_map)

# Initialize connection with database
# Create matrix to train
def InitDb(self):
    engine = create_engine("postgresql://"+self.user+":"+self.password+"@"+self.server+"/"+self.database+"")

    engineIS = create_engine("postgresql://"+self.user+":"+self.password+"@"+self.server+"/"+self.databaseIS+"")
    with engine.connect() as connection:
        result = connection.execute(text('select "VehicleTypeDetails"."Id"::text, "VehicleTypes"."Name" from "vehicleType"."VehicleTypeDetails" inner join "vehicleType"."VehicleTypes" on "vehicleType"."VehicleTypeDetails"."VehicleTypeId" = "vehicleType"."VehicleTypes"."Id"'))        
        
        # prop.items(): [('Name', 'SUV'), ('Id', '3a04991c-1039-2e22-c883-f3e1cd75892c')]
        # '3a04991c-103d-4263-bbaf-e8aefcda490d': ['Compact']
        # '3a04991c-103e-bbeb-f20d-9b66b7746644': ['Pick-up Truck']
        # '3a04991c-103e-bfe3-9d09-b6a1a224caef': ['Crew Cab']
        props = {}
        for item in [{column: value for column, value in prop.items()} for prop in result]:
         props.setdefault(item['Id'], []).append(item['Name'])
        
    with engine.connect() as connection:
      tag = connection.execute(text('SELECT "VehicleTypes"."Id"::text, "VehicleTypes"."Name" FROM "vehicleType"."VehicleTypes"'))
      categories = [{column: value for column, value in category.items()} for category in tag]
      propsList = list(props.items())

      # Create matrix to check and map each prop to category
      # ~ item propfile matrix
      a = np.asarray(list(map((lambda x: mapData(x, categories)), propsList)))
      numberOfItem = len(categories)
      
      X_train_counts = a[:, -(numberOfItem - 1):]

    with engineIS.connect() as connection:
      users = connection.execute(text('SELECT "AbpUsers"."Id"::text FROM public."AbpUsers";'))
      users = pd.DataFrame(users)

    # tfidf
    tfidf = ContentBase.getTfidf(X_train_counts)
    # get rate/favorites props for each user
    rate_train = getUserRatingMatrix(engine)

    d = tfidf.shape[1]  # data dimension
    n_users = users.shape[0]
    W = np.zeros((d, n_users))
    b = np.zeros((1, n_users))
    W, b = ContentBase.GetRidgeRegression(self=ContentBase, n_users=np.asarray(users), rate_train=rate_train,
                                          tfidf=tfidf, W=W, b=b, index_arr=a[:, 0])
    Yhat = tfidf.dot(W) + b
    return Yhat, users, a

def getUserRatingMatrix(engine):
  with engine.connect() as connection:
    #result = connection.execute(text('Select "userId"::text, "propertyId"::text, "rating" from user_rating'))
    result = connection.execute(text('SELECT "transaction"."UserTransactions"."UserId", "transaction"."UserTransactionVehicles"."VehicleId"  from "transaction"."UserTransactionVehicles" inner join "transaction"."UserTransactions" on "transaction"."UserTransactionVehicles"."UserTransactionId" = "transaction"."UserTransactions"."Id"'))
    test = [{column: value for column, value in rowproxy.items()} for rowproxy in result]
    df = pd.DataFrame(test)
    return df.values

async def serve() -> None:
  # Create GRPC Server
  server = grpc.aio.server()

  # Add Recommend Service to GRPC Server
  rs_pb2_grpc.add_RecommendationServicer_to_server(
    RecommendationServicer(), server)

  #listen_addr = '[::]:50051'
  listen_addr = "localhost:50051"
  server.add_insecure_port(listen_addr)
  logging.info("Starting server on %s", listen_addr)
  await server.start()
  await server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve())