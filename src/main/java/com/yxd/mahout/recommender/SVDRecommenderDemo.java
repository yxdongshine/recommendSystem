package com.yxd.mahout.recommender;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.DataModelBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

import java.io.File;
import java.io.IOException;
import java.util.List;


public class SVDRecommenderDemo {
    public static void main(String[] args) throws TasteException, IOException {
        // 一、加载数据
        DataModel dataModel =  new FileDataModel(new File("data/ratings_mahout.csv"));;
        // 二、给定矩阵分解的求解方式
        Factorizer factorizer = new ALSWRFactorizer(dataModel, 10, 0.01, 10);
        // 三、构建模型
        Recommender recommender = new SVDRecommender(dataModel, factorizer);

        // 获取推荐结果
        List<RecommendedItem> items = recommender.recommend(1, 10);
        for (RecommendedItem item : items) {
            System.out.println(item.getItemID() + ":" + item.getValue());
        }

        // 模型评估(根据RMS)的值来定义
        RecommenderBuilder builder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                Factorizer factorizer = new ALSWRFactorizer(dataModel, 10, 0.001, 10);
                Recommender recommender = new SVDRecommender(dataModel, factorizer);
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
        double score = evaluator.evaluate(builder, dataModelBuilder, dataModel, 0.7, 0.3);
        System.out.println("SVD score:" + score);
    }
}
