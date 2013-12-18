/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package crawler;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.Page;
import com.gargoylesoftware.htmlunit.RefreshHandler;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.DomNode;
import com.gargoylesoftware.htmlunit.html.HtmlAnchor;
import com.gargoylesoftware.htmlunit.html.HtmlMeta;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import java.io.*;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author Nhom 10 (Tran Trung Hieu, Tran Xuan Tu)
 */
public class Crawler {

    /**
     * @param args the command line arguments
     */
    public static String Url;

    public static DB articleDB;
    public static DBCollection coll;
    private BufferedReader reader ;
    private BufferedWriter writer ;
    public static Channel channel;
    public static void main(String[] args) throws IOException, InterruptedException {
        // TODO code application logic here
        final String initUrl = "http://vnexpress.net/tin-tuc/khoa-hoc"; 
        channel = new Channel();
        channel.Url = initUrl;
        channel.ListArticleXpath = "//a[@class = 'link-title14']";
        channel.NavigationXpath = "//a[@class='prve']";
        channel.pubDateXpath = "//meta[@name ='pubdate']";
        channel.titleXpath = "//div[@class=\"content\"]//h1[@class=\"Title\"]";
        channel.contentXpath = "//div[@class='fck_detail']/p";
        final int LIMIT = 100;
        final int NUM_OF_THREAD = 5 ;
        connectToMongodb();
        Crawler crawler = new Crawler() ;
        crawler.init(); 
        crawler.crawlExecute(channel.Url, LIMIT, NUM_OF_THREAD) ;
    }
    
    public void init() {
        try {
            reader = new BufferedReader(new FileReader(new File("urls.txt"))) ;
            writer = new BufferedWriter(new FileWriter("urls.txt", true)) ;
        } catch (IOException ex) {
            Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }
    
    private final WebClient createClient() {
        final WebClient webClient = new WebClient(BrowserVersion.getDefault());
        webClient.setRefreshHandler(new RefreshHandler() {
            @Override
            public void handleRefresh(Page page, URL url, int arg) throws IOException {}
        });
        webClient.getOptions().setJavaScriptEnabled(false);
        webClient.getOptions().setCssEnabled(false);
        webClient.getOptions().setTimeout(60000);
        return webClient ;
    }
    
    private List<String> getCurrentUrlList() throws IOException {
        List<String> urls = new ArrayList<String>();
        String url ;
        while((url = reader.readLine()) != null) {
            if(!url.isEmpty()) urls.add(url);
        }
        reader.close();
        return urls ;
    }
    
    private static void connectToMongodb() throws UnknownHostException
    {
        MongoClient mongoClient = new MongoClient("localhost");
        articleDB = mongoClient.getDB("Article");
        coll = articleDB.getCollection("vnexpressArticle");
    }
       
    private Article parseUrl(String articleUrl) throws IOException{
        final WebClient webClient = createClient() ;
        final HtmlPage page = webClient.getPage(articleUrl);
        
        Article article = new Article();
        article.Url = articleUrl;
        article.htmlContent = page.asXml();
        
        List<?> title = page.getByXPath(channel.titleXpath);
        if(!title.isEmpty())
        {
            article.Title = DomNode.class.cast(title.get(0)).asText();
        }
        
        List<?> date = page.getByXPath(channel.pubDateXpath);
        if(!date.isEmpty())
        {
            HtmlMeta hm = HtmlMeta.class.cast(date.get(0));  
            article.pubDate = hm.getContentAttribute();
        }
        
        List<?> content = page.getByXPath(channel.contentXpath);
        if(!content.isEmpty())
        {
            String text = "";
            for(int i = 0;i< content.size();i++)
            {
                text = text + DomNode.class.cast(content.get(i)).asText() + "\n";
            }
            article.Content = text;
        }
        
        return article;
    }
   
    private List<String> getUrlList(String InitUrl,int limit) throws IOException
    {
        List<String> resultList = new ArrayList<String>();
        URL Aurl = new URL(InitUrl);
        String hostname = Aurl.getProtocol()+"://"+ Aurl.getHost();
        String seedUrl = InitUrl;
        while(resultList.size() < limit){
            final WebClient webClient = createClient() ;
            final HtmlPage page = webClient.getPage(seedUrl);
            List<?> urlList = page.getByXPath(channel.ListArticleXpath);
            if(urlList.isEmpty() == false){
                for(int i = 0;i<urlList.size();i++)
                {
                    HtmlAnchor ha = HtmlAnchor.class.cast(urlList.get(i));
                    String url = ha.getHrefAttribute();
                    //writer.write(url);
                    resultList.add(url);
                }
            }
            List<?> navigation = page.getByXPath(channel.NavigationXpath);
            //System.out.println(deslist.size());
            if(navigation.isEmpty() ==  false )
            {
                    HtmlAnchor ha = HtmlAnchor.class.cast(navigation.get(0));
                    String url = ha.getHrefAttribute();
                    if(!url.startsWith(hostname)) url = hostname+url;
                    seedUrl = url;
            }
            webClient.closeAllWindows();
        }
        return resultList;
    }
    
    //multithreading crawl
    public void crawlExecute(String initUrl, int limit, int numOfThread) throws IOException, InterruptedException{
        List<String> newList = getUrlList(initUrl,limit);
        List<String> currenList = getCurrentUrlList() ;
        
        List<SingleCrawler> crwl = new ArrayList<SingleCrawler>();
        
        System.out.println("Total: "+ newList.size());
        int counter = 0;
        for(int i = 0; i < newList.size(); i++)
        {
            String newUrl = newList.get(i) ;
            //skip fetched url
            if(currenList.contains(newUrl)) continue ;

            //update new url
            writer.append(newUrl) ;
            crwl.add(new SingleCrawler(newUrl,limit));
            if(counter == numOfThread || i == newList.size() - 1) {
                for(SingleCrawler singCwl : crwl) {
                    singCwl.start();
                }
                
                for(SingleCrawler singCwl : crwl) {
                    singCwl.join();
                }
                
                counter = 0;
                crwl = new ArrayList<SingleCrawler>();
            }
            
            counter++ ;
        }
        writer.close();
    }
    
    private class SingleCrawler extends Thread{
        private String url ;
        
        public SingleCrawler(String url, int limit) {
            this.url = url ;
        }
        
        private void getArticle(String newUrl) throws IOException{
            //update new url 
            Article article = parseUrl(newUrl);
            BasicDBObject document = new BasicDBObject();
            document.put("Url", article.Url);
            document.put("Html", article.htmlContent);
            document.put("Title", article.Title);
            document.put("PubDate", article.pubDate);
            document.put("Content", article.Content);
            
            //skip empty document
            if(article.Title == null || article.Content == null) return ;
            //skip too short document
            if(article.Url.length() < 10 || article.Title.length() < 10 || article.Content.length() < 100) return ;
            //insert into mongoDB
            coll.insert(document);
        }
        
        @Override
        public void run() {
            System.out.println("Start thread to crawl + " + url);
            try {
                getArticle(url) ;
            } catch (IOException ex) {
                Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Finished and insert " +  url + " into MongoDB!");
        }
    }
}
