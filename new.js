var MongoClient = require('mongodb').MongoClient;
var crawler = require('./crawler');

var mongodb;
var postsCollection;
var usersCollection;

var PAGE_LIFE = 3;
var PAGE_TTL = 5;

var seeds = [
    //'http://qwerty1570.tumblr.com',
    'http://91bit.tumblr.com',
    'http://asianbeautyworld.tumblr.com',
    'http://smile67yt.tumblr.com'
];

var archiveData = {
    urls : "//x:div[contains(@class, 'is_photo') or contains(@class, 'is_video')]//x:a/@href",
    notes : "//x:div[contains(@class, 'is_photo') or contains(@class, 'is_video')]//x:span[@class='post_notes']/@data-notes",
    thumbs : "//x:div[contains(@class, 'is_photo') or contains(@class, 'is_video')]//x:div[contains(@class, 'post_thumbnail_container')]/@data-imageurl",
    nextArchive : "//x:a[@id='next_page_link']/@href"
};

var postData = {
    notes : "//x:ol[@class='notes']/x:li[not(contains(@class, 'more_notes_link_container'))]/x:a/@href",
    source : "//x:*[@id='posts']//x:a[contains(@class, 'source-link')]/@href"
};

function saveNewUser(user, callback)
{
    usersCollection.insert(user, {continueOnError: true}, callback);  
}

function saveNewPost(post, callback)
{
    postsCollection.update(
        {'url' : post.url},
        {$set : {
			'url' : post.url, 
			'likes' : parseInt(post.likes), 
			'thumb' : post.thumb, 
			'source' : post.source,
			'relation' : post.relation
			}
		},
        {upsert : true, safe : false},
        callback
    )    
}

function urlToUser(url)
{
	return url.slice(7, url.indexOf('.tumblr.com')); //http://xxxxxx.tumblr.com
}

function onPost(result)
{
    console.log('Post parsed: ' + result.priv.url + ', remain: ' + result.priv.ttl);
    
    if (result.priv.ttl - 1 > 0 && result.data.source != '') {
        var newUserUrl = 'http://' + urlToUser(result.data.source) + '.tumblr.com';
        saveNewUser({'url' : newUserUrl}, function(err, data) {
            if (err == undefined) {
                var page = {};
                page.data = archiveData;
                page.url = newUserUrl + '/archive';
                page.life = PAGE_LIFE;
                page.priv = {};
                page.priv.url = page.url;
                page.priv.ttl = result.priv.ttl - 1;
                page.priv.baseUrl = newUserUrl;
                page.onData = onArchive;
                crawler.pushPage(page);
            }
        })        
    }
    
    var post = {
        url : result.priv.url,
        likes : result.priv.likes,
        thumb : result.priv.thumb,
        source : result.data.source
    };
    
    post.relation = urlToUser(result.data.source) + ' ' + urlToUser(result.priv.url) + ' ';
    
    for (var i = 0; i < result.data.notes.length; i++) {
                var user = urlToUser(result.data.notes[i]);
                post.relation += user + ' ';
            }
            
    post.relation = post.relation.replace(/-/g, '');
    post.relation = post.relation.replace(/\./g, '');
    post.relation = post.relation.replace(/\//g, '');
            
    if (result.data.source != '')
        post.url = result.data.source;
    
    saveNewPost(post);
}

function onArchive(result)
{
    console.log('Archive parsed: ' + result.priv.url + ', remain: ' + result.priv.ttl);
    
    for (var i = 0; i < result.data.urls.length; i++) {
        var page = {};
        page.data = postData;
        page.onData = onPost;
        page.url = result.data.urls[i];
        page.life = PAGE_LIFE;
        page.priv = {};
        page.priv.url = page.url;
        page.priv.likes = result.data.notes[i];
        page.priv.thumb = result.data.thumbs[i];
        page.priv.ttl = result.priv.ttl;
        crawler.pushPage(page);
    }    
    
    if (result.nextArchive != '') {
        var page = {};
        page.data = archiveData;
        page.url = result.priv.baseUrl + result.data.nextArchive;
        page.life = PAGE_LIFE;
        page.priv = {};
        page.priv.ttl = result.priv.ttl;
        page.priv.url = page.url;
        page.priv.baseUrl = result.priv.baseUrl;
        page.onData = onArchive;
        crawler.pushPage(page);        
    }
}

function start() {
    var dbUrl = 'mongodb://localhost:27017/tumblr';
    //var dbUrl = 'mongodb://45.78.43.37:27017/tumblr';
    
	MongoClient.connect(dbUrl, function(err, db) {
		if (err) {
			console.log(err);
			return;
		}
		mongodb = db;
		usersCollection = db.collection('users');
		postsCollection = db.collection('posts');
		
		postsCollection.createIndex('url', { unique: true });
		usersCollection.createIndex('url', { unique: true });
		
		usersCollection.remove({});
        
        var seedUsers = [];
        for (var i = 0; i < seeds.length; i++)
            seedUsers.push({'url' : seeds[i]});
        
        
		saveNewUser(seedUsers, function(err, data) {
			for (var i = 0; i < seeds.length; i++) {
				var page = {};
                page.data = archiveData;
                page.url = seeds[i] + '/archive';
                page.life = PAGE_LIFE;
                page.priv = {};
                page.priv.ttl = PAGE_TTL;
                page.priv.baseUrl = seeds[i];
                page.priv.url = page.url;
                page.onData = onArchive;
                
				crawler.pushPage(page);
			}
		})
	});
}

start();

setInterval(function () {
    if (typeof gc === 'function') {
		console.log('###########Garbage Collection############')
        gc();
    }
}, 1000);
