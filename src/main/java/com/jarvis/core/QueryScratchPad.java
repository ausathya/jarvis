package com.jarvis.core;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class QueryScratchPad {

    private static Logger logger = Logger.getLogger(QueryScratchPad.class);

    private static final List<String> queries = new ArrayList<String>();

    private static final Map<String, Analyzer> analyzerPerField = new HashMap<String, Analyzer>();


    private static PerFieldAnalyzerWrapper analyzerWrapper = new PerFieldAnalyzerWrapper(new KeywordAnalyzer(), analyzerPerField);


    static {
        // description:iPhone
        queries.add("description:iPhone");
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
        // ( ( "Starbucks" OR "Tim Hortons" ) AND ( "coffee" ) ) AND NOT "tea"
        queries.add("( ( \"Starbucks\" OR \"Tim Hortons\" ) AND ( \"coffee\" ) ) AND NOT \"tea\"");
        // Medicare AND ("Humana" OR @Humana OR author:Humana OR author:Humana)
        queries.add("Medicare AND (\"Humana\" OR @Humana OR author:Humana OR author:Humana)");
        // "#theNew10" OR "#theNewTen" OR "#new10" OR "#newTen" OR ("the new 10" AND "woman")
        queries.add("\"#theNew10\" OR \"#theNewTen\" OR \"#new10\" OR \"#newTen\" OR (\"the new 10\" AND \"woman\")");
        // NOT ("dow Jones" OR stock OR stocks OR download OR downloads OR price OR prices OR tick OR ticks OR investor OR investors OR job OR jobs OR point OR points OR poor OR poorer) AND (dow OR "dow chemical" OR "dow company") AND (innovation OR innovations OR "carbon dioxide" OR Carbon OR footprint OR "life cycle" OR chemistry)
        queries.add("NOT (\"dow Jones\" OR stock OR stocks OR download OR downloads OR price OR prices OR tick OR ticks OR investor OR investors OR job OR jobs OR point OR points OR poor OR poorer) AND (dow OR \"dow chemical\" OR \"dow company\") AND (innovation OR innovations OR \"carbon dioxide\" OR Carbon OR footprint OR \"life cycle\" OR chemistry)");
        // "@thegame OR (((\"the game\" OR game) AND (\"sexual assault\" OR accused OR \"sexually assaulting\" OR \"Priscilla Rainey\" OR rainey OR @shesgotgameVH1 OR VH1 OR TMZ)) OR (@shesgotgameVH1 OR \"rapper the game\" OR \"rapper, the game\" OR \"Jayceon Terrell Taylor\" OR \"Jayceon Taylor\" OR jayceonterrelltaylor OR jayceontaylor) AND (VH1 OR TMZ OR gross OR disgusting OR sued OR \"sexual assault\" OR accused OR \"sexually assaulting\" OR \"Priscilla Rainey\" OR rainey OR rapper OR 10million OR 10mill OR 10m OR contestant)) OR ((rapper OR #shesgotgame) AND (rainey OR @shesgotgameVH1 OR \"priscilla rainey\" OR VH1))"
        queries.add("@thegame OR (((\"the game\" OR game) AND (\"sexual assault\" OR accused OR \"sexually assaulting\" OR \"Priscilla Rainey\" OR rainey OR @shesgotgameVH1 OR VH1 OR TMZ)) OR (@shesgotgameVH1 OR \"rapper the game\" OR \"rapper, the game\" OR \"Jayceon Terrell Taylor\" OR \"Jayceon Taylor\" OR jayceonterrelltaylor OR jayceontaylor) AND (VH1 OR TMZ OR gross OR disgusting OR sued OR \"sexual assault\" OR accused OR \"sexually assaulting\" OR \"Priscilla Rainey\" OR rainey OR rapper OR 10million OR 10mill OR 10m OR contestant)) OR ((rapper OR #shesgotgame) AND (rainey OR @shesgotgameVH1 OR \"priscilla rainey\" OR VH1))");
        // description:(("nike" OR "shoes") AND ("sports" OR "win"))
        queries.add("description:((\"nike\" OR \"shoes\") AND (\"sports\" OR \"win\"))");
        // description:(("nike" "shoes") AND ("sports" "win"))
        queries.add("description:((\"nike\" \"shoes\") AND (\"sports\" \"win\"))");
        // description:(bike OR nike OR reebok OR adidas OR jordan OR "dc shoes" OR "New balance" OR "under armour" OR saucony)
        queries.add("description:(bike OR nike OR reebok OR adidas OR jordan OR \"dc shoes\" OR \"New balance\" OR \"under armour\" OR saucony)");
    }

    private static final int maxQuery = 5;

    public static void main(String[] args) throws ParseException {
        displayLevels();
    }


    // Display
    public static void displayLevels() throws ParseException {
        QueryParser luceneQP = new QueryParser(Version.LUCENE_44, "text", analyzerWrapper);
        for(int i = 0; i < maxQuery; i++){
            String query = queries.get(i);
            Query parsedQuery = luceneQP.parse(query);
            displayQuery(query, parsedQuery);
        }
    }

    public static void displayQuery(String queryString, Query query){
        logger.info(String.format("Type:%-20s  Original: %-25s    Parsed: %s", query.getClass().getSimpleName(), queryString, query));
        if(query instanceof BooleanQuery){
            printBooleanQuery(0, (BooleanQuery)query);
        } else {
            printNonBooleanQuery(0, BooleanClause.Occur.SHOULD, query);
        }
    }

    public static void printBooleanQuery(int level, BooleanQuery query){
        //logger.debug("Min Match: " + query.getMinimumNumberShouldMatch());
        level++;
        for(BooleanClause booleanClause :  query.clauses()){
            if(booleanClause.getQuery() instanceof BooleanQuery) {
                logger.debug(getPrintPrefix(level) + booleanClause.getOccur().name());
                printBooleanQuery(level, (BooleanQuery) booleanClause.getQuery());
            } else {
                printNonBooleanQuery(level, booleanClause.getOccur(), booleanClause.getQuery());
            }
        }
    }

    public static void printNonBooleanQuery(int level, BooleanClause.Occur occur, Query query){
        logger.debug(String.format("%s%s %s  {%s}", getPrintPrefix(level), occur.name(), query.getClass().getSimpleName(), query));
    }

    private static final String prettyPrintPrefix = " ----- ";

    private static String getPrintPrefix(int level){
        String printPrefix = "";
        for(int i =1; i<level; i++) printPrefix += prettyPrintPrefix;
        return printPrefix + "> ";
    }

}
