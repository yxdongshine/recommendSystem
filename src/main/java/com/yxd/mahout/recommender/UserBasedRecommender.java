package com.yxd.mahout.recommender;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.DataModelBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.CityBlockSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.io.IOException;
import java.util.List;


/**
 * 参考官网文档:http://mahout.apache.org/
 * 用户推荐参考:http://mahout.apache.org/users/recommender/userbased-5-minutes.html
 */
public class UserBasedRecommender {
    public static void main(String[] args) throws IOException, TasteException {
        // 数据、如何计算用户的相似度、获取获取最近邻
        // 一、加载数据（可以支持本地文件或者JDBC的数据）
        DataModel model = new FileDataModel(new File("data/ratings_mahout.csv"));
        // 二、获取相似度计算公式
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        // 三、获取最近邻获取方式
        UserNeighborhood neighborhood = new NearestNUserNeighborhood(3, similarity, model);
        // 四、推荐模型构建
        long t1 = System.currentTimeMillis();
        Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
        long t2 = System.currentTimeMillis();

        // 五、数据推荐
        List<RecommendedItem> items = recommender.recommend(2, 5);
        long t3 = System.currentTimeMillis();
        for (RecommendedItem item : items) {
            System.out.println(item.getItemID() + ":" + item.getValue());
        }
        long t4 = System.currentTimeMillis();
        System.out.println((t2 - t1) + ":" + (t3 - t2) + ":" + (t4 - t3));

        System.out.println("=========================");
        long t5 = System.currentTimeMillis();
        items = recommender.recommend(1, 10);
        long t6 = System.currentTimeMillis();
        for (RecommendedItem item : items) {
            System.out.println(item.getItemID() + ":" + item.getValue());
        }
        long t7 = System.currentTimeMillis();
        System.out.println((t6 - t5) + ":" + (t7 - t6));


        // 获取推荐效果评估
        RecommenderBuilder builder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                UserSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);
                UserNeighborhood neighborhood = new NearestNUserNeighborhood(3, similarity, dataModel);
                Recommender recommender = new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
                return recommender;
            }
        };
        DataModelBuilder dataModelBuilder = new DataModelBuilder() {
            @Override
            public DataModel buildDataModel(FastByIDMap<PreferenceArray> trainingData) {
                return new GenericDataModel(trainingData);
            }
        };
        RecommenderEvaluator evaluator = new RMSRecommenderEvaluator();
        double score = evaluator.evaluate(builder, dataModelBuilder, model, 0.7, 0.3);
        System.out.println("User score:" + score);

    }
}
