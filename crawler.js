var request = require('request');
var async = require("async"); 
var domain = require('domain').create()

var xpath = require('xpath');
var parse5 = require('parse5');
var xmlser = require('xmlserializer');
var dom = require('xmldom').DOMParser;
var select = xpath.useNamespaces({"x": "http://www.w3.org/1999/xhtml"});

var TIME_OUT = 10000; //http request timeout
var REQUEST_CONCURRENCY = 100;
var queue = async.queue(parsePage, REQUEST_CONCURRENCY);
var pages = [];


domain.on('error', function(err) {
    // handle the error safely
    console.log(err)
})

function xmldomErrorHandler(err) {
    //suppress the useless errs or warnings
}

function getXmlDoc(html)
{
    var document = parse5.parse(html);
    var xhtml = xmlser.serializeToString(document);
    var options = {errorHandler : {warning:xmldomErrorHandler,error:xmldomErrorHandler,xmldomErrorHandler}};
    var doc = new dom(options).parseFromString(xhtml);
    return doc;
}

function extractData(path, doc)
{
    var node = select(path, doc);
    if (node.length == 1)
        return node[0].nodeValue;
    else if (node.length > 1) {
        var values = [];
        for (var i = 0; i < node.length; i++)
            values.push(node[i].nodeValue)
        
        return values;
    }
    return '';
}

function parsePage(page, next)
{
    request({uri: encodeURI(page.url), timeout: TIME_OUT}, function(err, resp, body) {
        if (err != undefined) {
            if (err.code === 'ETIMEDOUT' || err.code === 'ESOCKETTIMEDOUT')
                reschedulePage(page);
            else
                popPage(page);
            
            return;
        }
            
        var doc = getXmlDoc(body);
        
        var result = {};
        result.priv = page.priv;
        result.data = {};
        
        for (var i in page.data) {
            result.data[i] = extractData(page.data[i], doc);
        }
        
        page.onData(result);
        
        popPage(page);
        
        if (next != undefined)
            next();
    })
}

function reschedulePage(page)
{
    page.life--;
    if (page.life > 0) {
        queue.push(page);
    }
    else {
        console.log('Page abandoned: ' + page.url);
        popPage(page);
    }
}

function popPage(page)
{
    var index = pages.indexOf(page);
    if (index >= 0)
        pages.splice(index, 1);
}

function pushPage(page)
{
    pages.push(page);
    queue.push(page);
}

module.exports = {
    pushPage : pushPage,
}