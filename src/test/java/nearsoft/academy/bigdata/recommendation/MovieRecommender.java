package nearsoft.academy.bigdata.recommendation;

import com.google.common.collect.HashBiMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created by erick on 19/09/16.
 */
public class MovieRecommender {
    private int tReviews;
    private int tProducts;
    private int tUsers;
    private File cache = new File(System.getProperty("user.home")+File.separator+"cache.csv") ;
    private HashBiMap<String, Integer> productLikedID = HashBiMap.create();
    private HashBiMap<String, Integer> userLinkedID = HashBiMap.create();
    private UserBasedRecommender recommender;
    private List<RecommendedItem> recommendations;

    public MovieRecommender(String dataSourcePath) throws IOException, TasteException {
        if (cache.exists())
            cache.delete();
        else
            cache.createNewFile();
        System.out.println(cache.getAbsoluteFile());
        File source = new File(dataSourcePath);
        InputStream fileStream = new FileInputStream(source);
        InputStream gzipStream = new GZIPInputStream(fileStream);
        Reader decoder = new InputStreamReader(gzipStream, "UTF8");
        readDataAndCreateHash(decoder);
        DataModel model = new FileDataModel(new File(cache.getAbsolutePath()));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1,similarity,model);
        recommender = new GenericUserBasedRecommender(model,neighborhood,similarity);
    }


    public int getTotalReviews() throws IOException {
        if(tReviews==0)
            tReviews=countReviews();
        return tReviews;
    }

    public int getTotalProducts() {
        if (tProducts==0)
            tProducts=productLikedID.size();
        return tProducts;
    }

    public int getTotalUsers() {
        if (tUsers==0)
            tUsers=userLinkedID.size();
        return tUsers;
    }
    public int countReviews() throws IOException {
        int counter=0;
        LineIterator lt = FileUtils.lineIterator(cache,"UTF-8");
        while (lt.hasNext()){
            counter++;
            lt.nextLine();
        }
        LineIterator.closeQuietly(lt);
        return counter;
    }
    public void cacheToFile(int pID, int uID, String score) throws IOException {
        StringBuilder buffer = new StringBuilder();
        buffer.append(uID)
                .append(",")
                .append(pID)
                .append(",")
                .append(score);
        FileUtils.writeStringToFile(cache,buffer.toString(),true);
    }
    public void readDataAndCreateHash(Reader data) throws IOException {
        LineIterator lt = IOUtils.lineIterator(data);
        int pID=0;
        int uID=0;
        try {
            while (lt.hasNext()){
                String line=lt.nextLine();
                if (line.isEmpty()){
                    continue;
                }
                if (line.startsWith("product/productId")){
                    String productID = line.substring(19);
                    //FileUtils.writeStringToFile(cleanData,productID,true);
                    if (productLikedID.get(productID)==null){
                        productLikedID.put(productID,productLikedID.size()+1);
                    }
                    pID=productLikedID.get(productID);
                }
                if (line.startsWith("review/userId")){
                    String userID = line.substring(15);
                    //FileUtils.writeStringToFile(cleanData,userID,true);
                    if (userLinkedID.get(userID)==null){
                        userLinkedID.put(userID,userLinkedID.size()+1);
                    }
                    uID=userLinkedID.get(userID);

                }
                if (line.startsWith("review/score")){
                    String score = line.substring(14);
                    cacheToFile(pID,uID,score+"\n");
                }
            }
        } finally {
            LineIterator.closeQuietly(lt);

        }
    }

    public List<String> getRecommendationsForUser(String userId) {
        try {
            recommendations = recommender.recommend(userLinkedID.get(userId),3);
        } catch (TasteException e) {
            e.printStackTrace();
        }
        ArrayList<String> userRecommendations = new ArrayList<String>();
        for (RecommendedItem recommendedItem : recommendations){
            userRecommendations.add(productLikedID.inverse().get((int)recommendedItem.getItemID()));
        }
        return userRecommendations;
    }
}