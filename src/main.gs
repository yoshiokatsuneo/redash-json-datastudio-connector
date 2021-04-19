//  Copyright notice
//
//  by Tsuneo Yoshioka (yoshiokatsuneo@gmail.com)
//  Devlied from JSON connector by
//  (c) 2019 Gabriël Ramaker <gabriel@lingewoud.nl>, Lingewoud
//  ( https://github.com/googledatastudio/community-connectors/blob/master/JSON-connect/README.md )
//  All rights reserved
//
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
//  This copyright notice MUST APPEAR in all copies of the script!

/* eslint-disable prefer-rest-params */
/* eslint-disable prefer-spread */

/**
 * Throws and logs script exceptions.
 *
 * @param {String} message The exception message
 */
function sendUserError(message) {
  var cc = DataStudioApp.createCommunityConnector();
  cc.newUserError()
    .setText(message)
    .throwException();
}

/**
 * function  `getAuthType()`
 *
 * @returns {Object} `AuthType` used by the connector.
 */
function getAuthType() {
  return {type: 'NONE'};
}

/**
 * function  `isAdminUser()`
 *
 * @returns {Boolean} Currently just returns false. Should return true if the current authenticated user at the time
 *                    of function execution is an admin user of the connector.
 */
function isAdminUser() {
  return true;
}

/**
 * Returns the user configurable options for the connector.
 *
 * Required function for Community Connector.
 *
 * @param   {Object} request  Config request parameters.
 * @returns {Object}          Connector configuration to be displayed to the user.
 */
function getConfig(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();

  config
    .newInfo()
    .setId('instructions')
    .setText('Fill out the form to connect to a JSON data source.');

  config
    .newTextInput()
    .setId('url')
    .setName('Enter the URL of a JSON data source')
    .setHelpText('e.g. https://my-url.org/json')
    .setPlaceholder('https://my-url.org/json');

  config
    .newCheckbox()
    .setId('cache')
    .setName('Cache response')
    .setHelpText('Usefull with big datasets. Response is cached for 10 minutes')
    .setAllowOverride(true);

  config.setDateRangeRequired(false);

  return config.build();
}

/**
 * Gets UrlFetch response and parses JSON
 *
 * @param   {string} url  The URL to get the data from
 * @returns {Object}      The response object
 */
function fetchJSON(url) {
  try {
    var response = UrlFetchApp.fetch(url);
  } catch (e) {
    sendUserError('"' + url + '" returned an error:' + e);
  }

  try {
    var content = JSON.parse(response);
  } catch (e) {
    sendUserError('Invalid JSON format. ' + e);
  }

  return content;
}

/**
 * Gets cached response. If the response has not been cached, make
 * the fetchJSON call, then cache and return the response.
 *
 * @param   {string} url  The URL to get the data from
 * @returns {Object}      The response object
 */
function getCachedData(url) {
  var cacheExpTime = 600;
  var cache = CacheService.getUserCache();
  var cacheKey = url.replace(/[^a-zA-Z0-9]+/g, '');
  var cacheKeyString = cache.get(cacheKey + '.keys');
  var cacheKeys = cacheKeyString !== null ? cacheKeyString.split(',') : [];
  var cacheData = {};
  var content = [];

  if (cacheKeyString !== null && cacheKeys.length > 0) {
    cacheData = cache.getAll(cacheKeys);

    for (var key in cacheKeys) {
      if (cacheData[cacheKeys[key]] != undefined) {
        content.push(JSON.parse(cacheData[cacheKeys[key]]));
      }
    }
  } else {
    content = fetchJSON(url);

    for (var key in content) {
      cacheData[cacheKey + '.' + key] = JSON.stringify(content[key]);
    }

    cache.putAll(cacheData);
    cache.put(cacheKey + '.keys', Object.keys(cacheData), cacheExpTime);
  }

  return content;
}

/**
 * Fetches data. Either by calling getCachedData or fetchJSON, depending on the cache configuration parameter.
 *
 * @param   {String}  url   The URL to get the data from
 * @param   {Boolean} cache Parameter to determine whether the request should be cached
 * @returns {Object}        The response object
 */
function fetchData(url, cache) {
  if (!url || !url.match(/^https?:\/\/.+$/g)) {
    sendUserError('"' + url + '" is not a valid url.');
  }
  try {
    var content = cache ? getCachedData(url) : fetchJSON(url);
  } catch (e) {
    sendUserError(
      'Your request could not be cached. The rows of your dataset probably exceed the 100KB cache limit.'
    );
  }
  if (!content) sendUserError('"' + url + '" returned no content.');

  return content;
}

/**
 * Matches the field value to a semantic
 *
 * @param   {Mixed}   value   The field value
 * @param   {Object}  types   The list of types
 * @return  {string}          The semantic type
 */
function getSemanticType(type, types) {
  /*
  https://developers.google.com/apps-script/reference/data-studio/field-type
  */
  if (type === 'string') {
    return types.TEXT;
  } else if (type === 'integer') {
    return types.NUMBER;
  } else if (type === 'float') {
    return types.NUMBER;
  } else if (type === 'boolean') {
    return types.BOOLEAN;
  } else if (type === 'datetime') {
    return types.YEAR_MONTH_DAY_SECOND;
  } else {
    console.log("getSemanticType: Uknown type", type, types);
    return types.TEXT;
  }
}

/**
 *  Creates the fields
 *
 * @param   {Object}  fields  The list of fields
 * @param   {Object}  types   The list of types
 * @param   {String}  key     The key value of the current element
 * @param   {Mixed}   value   The value of the current element
 */
function createField(fields, types, column, count) {
  console.log("createField", fields, types, column, count);
  var type = column['type'];
  var semanticType = getSemanticType(type, types);
  var key = column['friendly_name'];
  var field =
    semanticType == types.NUMBER ? fields.newMetric() : fields.newDimension();
  var id = key.replace(/\s/g, '_').toLowerCase();
  if(id ==  ""){
    id = count.toString();
  }
  console.log("createField: semanticType, id, key=", semanticType, id, key);
  field.setType(semanticType);
  field.setId(id);
  field.setName(key);
}

/**
 * Handles keys for recursive fields
 *
 * @param   {String}  currentKey  The key value of the current element
 * @param   {Mixed}   key         The key value of the parent element
 * @returns {String}  if true
 */
function getElementKey(key, currentKey) {
  if (currentKey == '' || currentKey == null) {
    return;
  }
  if (key != null) {
    return key + '.' + currentKey.replace('.', '_');
  }
  return currentKey.replace('.', '_');
}

/**
 * Extracts the objects recursive fields and adds it to fields
 *
 * @param   {Object}  fields  The list of fields
 * @param   {Object}  types   The list of types
 * @param   {String}  key     The key value of the current element
 * @param   {Mixed}   value   The value of the current element
 * @param   {boolean} isInline if true
 */
function createFields(fields, types, columns) {
  columns.forEach(function(column, count){
    createField(fields, types, column, count);
  });
}

/**
 * Parses first line of content to determine the data schema
 *
 * @param   {Object}  request getSchema/getData request parameter.
 * @param   {Object}  content The content object
 * @return  {Object}           An object with the connector configuration
 */
function getFields(request, columns) {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  createFields(fields, types, columns);
  return fields;
}

/**
 * Returns the schema for the given request.
 *
 * @param   {Object} request Schema request parameters.
 * @returns {Object} Schema for the given request.
 */
function getSchema(request) {
  var content = fetchData(request.configParams.url, request.configParams.cache);
  var columns = content["query_result"]["data"]["columns"];
  var fields = getFields(request, columns).build();
  return {schema: fields};
}

/**
 * Returns the (nested) values for requested columns
 *
 * @param   {Object} valuePaths       Field name. If nested; field name and parent field name
 * @param   {Object} row              Current content row
 * @returns {Mixed}                   The field values for the columns
 */
function getColumnValue(valuePaths, row) {
  for (var index in valuePaths) {
    var currentPath = valuePaths[index];

    if (row[currentPath] === null) {
      return '';
    }

    if (row[currentPath] !== undefined) {
      row = row[currentPath];
      continue;
    }
    var keys = Object.keys(row);

    for (var index_keys in keys) {
      var key = keys[index_keys].replace(/\s/g, '_').toLowerCase();
      if (key == currentPath) {
        row = row[keys[index_keys]];
        break;
      }
    }
  }
  return row;
}

/**
 * Returns an object containing only the requested columns
 *
 * @param   {Object} content          The content object
 * @param   {Object} requestedFields  Fields requested in the getData request.
 * @returns {Object}                  An object only containing the requested columns.
 */
function getRows(content, requestedFields) {
  var cc = DataStudioApp.createCommunityConnector();
  var types = cc.FieldType;

  var rows = content.map(function(row) {
    var rowValues = [];

    requestedFields.asArray().forEach(function(field) {
      var fieldName = field.getName();
      var fieldValue = row[fieldName];
      var fieldType = field.getType();
      if (fieldType === types.YEAR_MONTH_DAY_SECOND){
        fieldValue = fieldValue.replace(/(....)-(..)-(..)T(..):(..):(..).*/, '$1$2$3$4$5$6');
      }
      // console.log("getRows: field, fieldName, fieldValue=", field, fieldName, fieldValue);
      rowValues.push(fieldValue);
    });
    return {values: rowValues};
  });
  // console.log("getRows returns" ,  rows);
  return rows;
}

/**
 * Returns the tabular data for the given request.
 *
 * @param   {Object} request  Data request parameters.
 * @returns {Object}          Contains the schema and data for the given request.
 */
function getData(request) {
  var content = fetchData(request.configParams.url, request.configParams.cache);
  var rows = content["query_result"]["data"]["rows"];
  var columns = content["query_result"]["data"]["columns"];
  var fields = getFields(request, columns);
  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  var requestedFields = fields.forIds(requestedFieldIds);
  console.log("getData: request.fields=", request.fields);
  console.log("getData: requestedFieldIds=", requestedFieldIds);
  console.log("getData: requestedFields=", requestedFields);
  
  return {
    schema: requestedFields.build(),
    rows: getRows(rows, requestedFields)
  };
}