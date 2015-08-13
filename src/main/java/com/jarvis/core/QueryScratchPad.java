package com.jarvis.core;

import com.jarvis.core.criteria.TermCriteria;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.Version;

import java.util.*;

import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;


public class QueryScratchPad {

    private static Logger logger = Logger.getLogger(QueryScratchPad.class);

    private static final List<String> queries = new ArrayList<String>();

    private static final Map<String, Analyzer> analyzerPerField = new HashMap<String, Analyzer>();

    private static PerFieldAnalyzerWrapper analyzerWrapper = new PerFieldAnalyzerWrapper(new KeywordAnalyzer(), analyzerPerField);

    static {
        // description:iPhone
        queries.add("iPhone");
        // Microsoft AND Google
        queries.add("Microsoft AND Google");
        // "Perdue" AND "antibiotics"
        queries.add("\"Perdue\" AND \"antibiotics\"");
        // ("british columbia" OR BC) AND "wild within"
        queries.add("(\"british columbia\" OR BC) AND \"wild within\"");
        // "nwnatural" OR "nw natural" OR "northwest natural"
        queries.add("\"nwnatural\" OR \"nw natural\" OR \"northwest natural\"");
        // ( ( "dow" OR "chemical" ) AND ( "canada" ) ) AND NOT "dow jones"
        queries.add("( ( \"dow\" OR \"chemical\" ) AND ( \"canada\" ) ) AND NOT \"dow jones\"");
        // ( ( "Starbucks" OR "Tim Hortons" ) AND ( "coffee" ) ) AND "tea"
        queries.add("( Starbucks OR Hortons ) AND ( coffee )");
        // Medicare AND ("Humana" OR @Humana OR author:Humana OR author:Humana)
        queries.add("Medicare AND (\"Humana\" OR @Humana OR author:Humana OR author:Humana)");
        // "#theNew10" OR "#theNewTen" OR "#new10" OR "#newTen" OR ("the new 10" AND "woman")
        queries.add("\"#theNew10\" OR \"#theNewTen\" OR \"#new10\" OR \"#newTen\" OR (\"the new 10\" AND \"woman\")");
        // NOT ("dow Jones" OR stock OR stocks OR download OR downloads OR price OR prices OR tick OR ticks OR investor OR investors OR job OR jobs OR point OR points OR poor OR poorer) AND (dow OR "dow chemical" OR "dow company") AND (innovation OR innovations OR "carbon dioxide" OR Carbon OR footprint OR "life cycle" OR chemistry)
        queries.add("NOT (stocks OR download OR prices OR tick OR investors OR job OR points OR poor) AND (dow OR \"dow chemical\") AND (innovation OR Carbon OR footprint OR \"life cycle\" OR chemistry)");
        // "@thegame OR (((\"the game\" OR game) AND ( accused OR rainey OR @shesgotgameVH1 OR VH1 OR TMZ)) OR (@shesgotgameVH1 OR jayceonterrelltaylor OR jayceontaylor) AND (VH1 OR TMZ OR gross OR disgusting OR sued OR rainey OR rapper)) OR ((rapper OR #shesgotgame) AND (rainey OR @shesgotgameVH1 OR VH1))"
        queries.add("@thegame OR (((\"the game\" OR game) AND (accused OR rainey OR @shesgotgameVH1 OR VH1 OR TMZ))" +
                " OR (@shesgotgameVH1 OR \"rapper the game\" OR \"rapper, the game\" OR jayceonterrelltaylor OR jayceontaylor) " +
                "AND (VH1 OR TMZ OR gross OR disgusting OR sued  OR accused  OR rainey OR rapper OR 10million OR 10mill OR 10m OR contestant)) " +
                "OR ((rapper OR #shesgotgame) AND (rainey OR @shesgotgameVH1 OR VH1))");
        // description:(("nike" OR "shoes") AND ("sports" OR "win"))
        queries.add("description:((\"nike\" OR \"shoes\") AND (\"sports\" OR \"win\"))");
        // description:(("nike" "shoes") AND ("sports" "win"))
        queries.add("description:((\"nike\" \"shoes\") AND (\"sports\" \"win\"))");
        // description:(bike OR nike OR reebok OR adidas OR jordan OR "dc shoes" OR "New balance" OR "under armour" OR saucony)
        queries.add("description:(bike OR nike OR reebok OR adidas OR jordan OR \"dc shoes\" OR \"New balance\" OR \"under armour\" OR saucony)");
    }

    private static final int maxQuery = queries.size();

    private static final int noOfIterations = 1;

    public static void main(String[] args) throws ParseException {
        develop();
        //measure();
    }

    private static void develop() throws ParseException{
        for(int i = 0; i < maxQuery; i++){
            buildCriteriaTree(queries.get(i));
            if(logger.isDebugEnabled())logger.debug("\n\n\n");
        }
    }

    private static void measure() throws ParseException{
        // Warm-Up
        buildCriteriaTree();
        final int queriesPerIteration = maxQuery;
        final int totalQueries = noOfIterations * queriesPerIteration;
        logger.info("Start measuring time.");
        long startTime = System.currentTimeMillis();
        for(int i = 1; i <= noOfIterations; i++){
            buildCriteriaTree();
        }
        long executionTimeInMillis = getElapsedTime(startTime);
        long timePerQueryInMillis = executionTimeInMillis/totalQueries;
        long qps = ( timePerQueryInMillis > 0 ) ? (1000/timePerQueryInMillis) : 1000; // if time per query is less than one millis
        logger.info(String.format("Total Time: %5d  |   Total Queries: %5d  |   Time/Query:%5d  | QPS: %d/sec", executionTimeInMillis, totalQueries, timePerQueryInMillis, qps));
    }

    public static void buildCriteriaTree() throws ParseException {
        for(int i = 0; i < maxQuery; i++){
            buildCriteriaTree(queries.get(i));
            if(logger.isDebugEnabled())logger.debug("\n\n\n");
        }
    }

    private static final QueryParser QUERY_PARSER = new QueryParser(Version.LUCENE_44, "text", analyzerWrapper);

    //  ---------------------   More like Pseudo-Tree    ---------------------
    public static List<TermCriteria> buildCriteriaTree(String qString) throws ParseException{
        List<TermCriteria> mergedTCList;
        Query q = QUERY_PARSER.parse(qString);
        if(logger.isDebugEnabled()) logger.debug(String.format("Type:%-20s  Original: %-25s    Parsed: %s", q.getClass().getSimpleName(), qString, q));
        if(q instanceof BooleanQuery){
            BooleanQuery bq = (BooleanQuery) q;
            List<List<TermCriteria>> queryTCList = new ArrayList<List<TermCriteria>>();
            Occur clause = SHOULD;
            for(BooleanClause bc :  bq.clauses()) {
                if(bc.getOccur() == Occur.MUST) {
                    clause = Occur.MUST;
                }
                queryTCList.add(processBooleanClause(0, bc, bq));
            }
            if(clause == Occur.MUST) {
                mergedTCList = mergeConjunction(queryTCList);
            } else {
                mergedTCList = mergeDisjunction(queryTCList);
            }
        } else {
            mergedTCList = buildNonBQCriteria(0, q);
        }
        logger.info(String.format("Total Criteria: %d", mergedTCList.size()));
        displayCriteriaList(mergedTCList);
        return mergedTCList;
    }

    private static List<TermCriteria> processBooleanClause(int level, BooleanClause bc, BooleanQuery bq){
        List<TermCriteria> mergedTCList;
        if(bc.getQuery() instanceof BooleanQuery) {
            if(logger.isDebugEnabled())logger.debug(String.format("%s%s %s  Parent:{%s}    ****", getPrintPrefix(level), bc.getOccur().name(), bq.getClass().getSimpleName(), bq));
            level++;
            BooleanQuery childBQ = (BooleanQuery) bc.getQuery();
            List<List<TermCriteria>> queryTCList = new ArrayList<List<TermCriteria>>();
            Occur clause = SHOULD;
            for(BooleanClause childBC : childBQ.clauses()){
                if(childBC.getOccur() == Occur.MUST) {
                    clause = Occur.MUST;
                }
                queryTCList.add(processBooleanClause(level, childBC, childBQ));
            }
            if(clause == Occur.MUST) {
                mergedTCList = mergeConjunction(queryTCList);
            } else {
                mergedTCList = mergeDisjunction(queryTCList);
            }
        } else {
            mergedTCList = buildNonBQCriteria(level, bc, bc.getQuery());
        }
        if(logger.isDebugEnabled())logger.debug("Intermediate merged list for:" + String.format("Type:%-20s  Parsed: %s", bq.getClass().getSimpleName(), bq));
        if(logger.isDebugEnabled())displayCriteriaList(mergedTCList);
        return mergedTCList;
    }

    private static List<TermCriteria> mergeDisjunction(List<List<TermCriteria>> queryTCList){
        List<TermCriteria> finalTCList = new ArrayList<TermCriteria>();
        if(queryTCList.size() == 0) return finalTCList;
        if(queryTCList.size() == 1) return queryTCList.get(0);
        for(List<TermCriteria> tcList : queryTCList){
            for(TermCriteria tc : tcList){
                if(logger.isDebugEnabled())logger.debug("Add OR Criteria: " + tc);
                finalTCList.add(tc);
            }
        }

        return finalTCList;
    }

    private static List<TermCriteria> mergeConjunction(List<List<TermCriteria>> queryTCList){
        List<TermCriteria> finalTCList = new ArrayList<TermCriteria>();
        if(queryTCList.size() == 0) return finalTCList;
        if(queryTCList.size() == 1) return queryTCList.get(0);
        Iterator<List<TermCriteria>> conjunctions = queryTCList.iterator();
        finalTCList = conjunctions.next();
        while(conjunctions.hasNext()){
            finalTCList = mergeConjuction(finalTCList, conjunctions.next());
        }
        return finalTCList;
    }

    private static List<TermCriteria> mergeConjuction(List<TermCriteria> listA, List<TermCriteria> listB){
        final int size = ( listA.size() >= listB.size() ) ? listA.size() : listB.size();
        List<TermCriteria> mergedTCList = new ArrayList<TermCriteria>(size);
        for(TermCriteria tcA : listA){
            for(TermCriteria tcB : listB){
                TermCriteria cloneOfTCA = TermCriteria.clone(tcA);
                TermCriteria cloneOfTCB = TermCriteria.clone(tcB);
                cloneOfTCA.merge(cloneOfTCB);
                if(logger.isDebugEnabled())logger.debug("Add AND Criteria: " + cloneOfTCA);
                mergedTCList.add(cloneOfTCA);
            }
        }
        return mergedTCList;
    }

    public static List<TermCriteria> buildNonBQCriteria(int level, Query q){
        return buildNonBQCriteria(level, SHOULD, q);
    }

    public static List<TermCriteria> buildNonBQCriteria(int level, BooleanClause bc, Query q){
        return buildNonBQCriteria(level, bc.getOccur(), q);
    }

    public static List<TermCriteria> buildNonBQCriteria(int level, Occur occur, Query q) {
        if(logger.isDebugEnabled())logger.debug(String.format("%s%s %s  {%s}", getPrintPrefix(level), occur.name(), q.getClass().getSimpleName(), q));
        List<TermCriteria> termCriteriaList = new ArrayList<TermCriteria>(2);
        if(q instanceof TermQuery){
            termCriteriaList.add(createTermCriteria((TermQuery) q));
        } else if(q instanceof PhraseQuery){
            termCriteriaList.addAll(createTermCriteria((PhraseQuery) q));
        }
        return termCriteriaList;
    }

    private static TermCriteria createTermCriteria(TermQuery tq){
        Term t = tq.getTerm();
        return new TermCriteria(t.text(), t.field());
    }

    private static List<TermCriteria> createTermCriteria(PhraseQuery pq){
        Term[] terms = pq.getTerms();
        List<TermCriteria> tcList = new ArrayList<TermCriteria>(terms.length);
        for(Term t : terms){
            TermCriteria tc = new TermCriteria(t.text(), t.field());
            tcList.add(tc);
        }
        return tcList;
    }


    private static void displayCriteriaList(List<TermCriteria> termCriteriaList){
        if(logger.isDebugEnabled())logger.debug("Total List of Criteria: " + termCriteriaList.size());
        for(TermCriteria termCriteria : termCriteriaList) {
            if(logger.isDebugEnabled())logger.debug(termCriteria);
        }
    }


    private static final String prettyPrintPrefix = " ----- ";

    private static String getPrintPrefix(int level){
        String prefix = "";
        for(int i = 0; i < level; i++) prefix += prettyPrintPrefix;
        return  String.format(" [%d] %s > ", level, prefix);
    }

    public static long getElapsedTime(long timeInMilliSecs){
        return System.currentTimeMillis() - timeInMilliSecs;
    }

}
