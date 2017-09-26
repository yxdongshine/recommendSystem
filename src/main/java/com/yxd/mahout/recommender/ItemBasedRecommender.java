package com.yxd.mahout.recommender;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.DataModelBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.impl.similarity.CityBlockSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.io.File;
import java.io.IOException;
import java.util.List;


public class ItemBasedRecommender {
    public static void main(String[] args) throws IOException, TasteException {
        // 一、加载数据
        DataModel model = new FileDataModel(new File("data/ratings_mahout.csv"));
        // 二、获取物品相似度比较的公式
        ItemSimilarity similarity = new CityBlockSimilarity(model);
        // 三、获取推荐系统对象
        Recommender recommender = new GenericItemBasedRecommender(model, similarity);

        // 获取推荐结果
        List<RecommendedItem> items = recommender.recommend(1, 10);
        for (RecommendedItem item : items) {
            System.out.println(item.getItemID() + ":" + item.getValue());
        }


        // 获取推荐效果评估
        RecommenderBuilder builder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                ItemSimilarity similarity = new CityBlockSimilarity(dataModel);
//                ItemSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);
                Recommender recommender = new GenericItemBasedRecommender(dataModel, similarity);
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
        System.out.println("Item score:" + score);
    }
}
