from flask import Flask, jsonify, request
import json
import happybase
import struct

app = Flask(__name__)

connection = happybase.Connection('rcg-hadoop')
tableUser = connection.table('StackOver_User_2')
tableTag = connection.table('StackOver_Tag_2')
USER_LIMIT = 5

def getScore(tags,userTags):
    scores = 0
    tags = dict(map(lambda(s,t):(t,s),tags))
    for tag in userTags:
        if tag in tags:
            scores += tags[tag]
    return float("{0:.4f}".format(scores))

def getUserProfile(userList):
    guildDict = dict(map(lambda (s,id):(id,s),userList))
    guildIds = map(lambda (s,id):id,userList)
    userProfileList = []
    for key, data in tableUser.rows(guildIds,columns=['Profile_F:Profile']):
        profileD = json.loads(data['Profile_F:Profile'])
        profileD['GuildScore'] = guildDict[key]
        userProfileList.append(profileD)
    di = {}
    di['users'] =userProfileList
    return json.dumps(di)


def getUserScore(ids,tags):
    tagScore = []
    for key, data in tableUser.rows(ids,columns=['Tags_F:Tags']):
        tagsScore = json.loads(data['Tags_F:Tags'])['Tags']
        score = getScore(tagsScore,tags)
        tagScore.append((score,key))
    tagScore = sorted(tagScore,reverse = True)
    if(len(tagScore) > USER_LIMIT):
        tagScore = tagScore[0:USER_LIMIT]
    return getUserProfile(tagScore)

def getUsers(tags):
    listUsers = []
    try:
        for key, data in tableTag.rows(tags):
            temp = data['Score_F:Score']
            temp = json.loads(temp)
            ids = map(lambda (score,id):str(id),temp['Users'])
            listUsers = listUsers + ids
        listUsers = list(set(listUsers))
        return getUserScore(listUsers,tags)
    except Exception,e:
        print str(e)
        @app.route('/', methods=['GET'])

def get_tasks():
    tags = request.args.getlist('tag')
    data = getUsers(tags)
    return data

if __name__ == '__main__':
    app.run(host= '0.0.0.0')