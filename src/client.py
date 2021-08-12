import os
import uuid
import json
import time
import pycurl
import certifi
import pandas as pd

from io import BytesIO
from io import StringIO

class EcClient:

    def __init__( self, path, max_cost=0 ):

        """
        constructor
        """

        # read api key into string
        with open( os.path.join( path, 'key.txt' ), ) as f:
            self._api_key = f.readline()
        
        # dictionary of api uris
        self._uris = {  'archive' : 'https://api.skywatch.co/earthcache/archive',
                        'base' : 'https://api.skywatch.co/earthcache' }

        # load template json objects from file            
        self._templates = dict()
        for key in [ 'search', 'pipeline' ]:

            with open( os.path.join( path, f'{key}.json' ), ) as f:
                self._templates[ key ] = json.load( f )

        # copy args
        self._max_cost = max_cost

        return


    def postSearch( self, aoi, window, **kwargs ):

        """
        post search
        """

        def getPayload():

            """
            get payload
            """

            # configure payload
            payload =  self._templates[ 'search' ]
            payload[ 'location' ] = aoi

            # get time range
            payload[ 'start_date' ] = window[ 'start_date' ]
            payload[ 'end_date' ] = window[ 'end_date' ]

            # for each template field
            for key in payload.keys():

                # replace tenplate values with kwargs
                value = kwargs.get( key )
                if value is not None:
                    payload[ key ] = value

            return payload

        # get request
        search_id = None
        request = self.initRequest( self._uris[ 'archive' ] + '/search' )

        # get payload
        payload = json.dumps( getPayload() )

        # prepare post
        request.setopt( pycurl.POST, 1)
        request.setopt( pycurl.READDATA, StringIO( payload ) )
        request.setopt( pycurl.POSTFIELDSIZE, len( payload ) )

        # capture response
        response = BytesIO()
        request.setopt( pycurl.WRITEFUNCTION, response.write )

        # execute request
        request.perform()

        # get status 
        status = request.getinfo(pycurl.RESPONSE_CODE ) 
        if status == 200:

            # parse response for search id
            obj = json.loads( response.getvalue() )
            if 'data' in obj:
                search_id = obj[ 'data' ][ 'id' ]

        # return status code and response 
        return search_id, status, json.loads( response.getvalue() )


    def getSearch( self, search_id ):

        """
        get search
        """

        # execute get request
        return self.sendRequest( self._uris[ 'archive' ] + f'/search/{search_id}/search_results' )


    def processSearch( self, aoi, window, **kwargs ):

        """
        process search
        """

        # get delay
        delay = kwargs.get( 'delay', 2 )
        result = None

        # post search job
        search_id, status, _ = self.postSearch( aoi, window, **kwargs )
        if status == 200 and search_id is not None:

            # loop until error or search complete
            while True:

                # get search result
                status, result = client.getSearch( search_id )
                if status != 202:
                    break

                # delay between get requests
                time.sleep( delay )
        
        return status, result, search_id


    def getPipelines( self ):

        """
        get pipelines
        """

        # run get request
        return self.sendRequest( self._uris[ 'base' ] + '/pipelines' )


    def getPipeline( self, pipeline_id ):

        """
        get pipeline associated with id
        """

        # run get request
        return self.sendRequest( self._uris[ 'base' ] + f'/pipelines/{pipeline_id}' )


    def deletePipeline( self, pipeline_id ):

        """
        delete pipeline
        """

        # run custom delete request
        return self.sendRequest( self._uris[ 'base' ] + f'/pipelines/{pipeline_id}', action='DELETE' )


    def createPipeline( self, aoi, window, **kwargs ):

        """
        create pipeline
        """

        def getPayload():

            """
            get payload
            """

            # configure payload
            payload =  self._templates[ 'pipeline' ]
            payload[ 'aoi' ] = aoi

            # get time range
            payload[ 'start_date' ] = window[ 'start_date' ]
            payload[ 'end_date' ] = window[ 'end_date' ]

            # assign max cost
            payload[ 'max_cost' ] = self._max_cost

            # for each template field
            keys = list( payload.keys() ) + [ 'search_id', 'search_results' ]
            for key in keys:

                # replace tenplate values with kwargs
                value = kwargs.get( key )
                if value is not None:
                    payload[ key ] = value

            return payload

        # create request
        request = self.initRequest( self._uris[ 'base' ] + '/pipelines' )

        # get payload
        payload = json.dumps( getPayload() )

        # prepare post
        request.setopt( pycurl.POST, 1)
        request.setopt( pycurl.READDATA, StringIO( payload ) )
        request.setopt( pycurl.POSTFIELDSIZE, len( payload ) )

        # capture response
        response = BytesIO()
        request.setopt( pycurl.WRITEFUNCTION, response.write )

        # execute request
        request.perform()

        # return status code and response
        return request.getinfo(pycurl.RESPONSE_CODE ), json.loads( response.getvalue() )


    def initRequest( self, uri ):

        """
        get request
        """

        # setup curl object - add ssl certification
        request = pycurl.Curl()
        request.setopt( pycurl.CAINFO, certifi.where() )

        # add uri + header
        request.setopt( pycurl.URL, uri )
        request.setopt( pycurl.HTTPHEADER, self.getHeaderParams() ) 

        return request


    def sendRequest( self, uri, action='GET', status_ok=[ 200 ] ):

        """
        get request
        """

        # create request
        request = self.initRequest( uri )

        # configure request type
        if action == 'GET':
            request.setopt( pycurl.HTTPGET, 1)
        else:
            request.setopt( pycurl.CUSTOMREQUEST, action )

        # capture response to request
        response = BytesIO()
        request.setopt( pycurl.WRITEFUNCTION, response.write )

        # execute request
        request.perform()

        # return status and response
        return request.getinfo(pycurl.RESPONSE_CODE), json.loads( response.getvalue() )


    def getHeaderParams( self ):

        """
        get header parameters + values
        """

        # return header attributes
        return [    'Accept: application/json',
                    'Content-Type: application/json',
                    'x-api-key: {key}'.format( key=self._api_key ) ]

