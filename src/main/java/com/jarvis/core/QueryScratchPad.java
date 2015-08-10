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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private static final int maxQuery = 7;

    public static void main(String[] args) throws ParseException {
        buildCriteriaTree();
    }


    public static void buildCriteriaTree() throws ParseException {
        List<TermCriteria> tcList = new ArrayList<TermCriteria>();
        QueryParser luceneQP = new QueryParser(Version.LUCENE_44, "text", analyzerWrapper);
        for(int i = 6; i < maxQuery; i++){
            String qString = queries.get(i);
            Query q = luceneQP.parse(qString);
            buildCriteriaTree(qString, q, tcList);
        }
        displayCriteriaList(tcList);
    }

    private static void displayCriteriaList(List<TermCriteria> termCriteriaList){
        for(TermCriteria termCriteria : termCriteriaList){
            logger.info(String.format("TermCriteria Term:%-20s  Fields: %s", termCriteria.getTerm(), termCriteria.getMustFields()));
        }
    }

    //  ---------------------   More like Pseudo-Tree    ---------------------
    public static void buildCriteriaTree(String qString, Query q, List<TermCriteria> tcList){
        logger.info(String.format("Type:%-20s  Original: %-25s    Parsed: %s", q.getClass().getSimpleName(), qString, q));
        if(q instanceof BooleanQuery){
            BooleanQuery bq = (BooleanQuery) q;
            for(BooleanClause bc :  bq.clauses()) {
                if(bc.getQuery() instanceof BooleanQuery) {
                    buildBQCriteria(0, bc, (BooleanQuery) bc.getQuery(), tcList);
                } else {
                    buildNonBQCriteria(0, bc, bc.getQuery());
                }
            }
        } else {
            tcList.addAll(buildNonBQCriteria(0, q));
        }
    }

    public static void buildBQCriteria(int level, BooleanClause parentBC, BooleanQuery query, List<TermCriteria> tcList){
        logger.debug(String.format("%s%s %s  {%s}", getPrintPrefix(level), parentBC.getOccur().name(), query.getClass().getSimpleName(), query));
        level++;
        for(BooleanClause bc :  query.clauses()){
            if(bc.getQuery() instanceof BooleanQuery) {
                buildBQCriteria(level, bc, (BooleanQuery) bc.getQuery(), tcList);
            } else {
                buildNonBQCriteria(level, bc, bc.getQuery());
            }
        }
    }

    public static List<TermCriteria> buildNonBQCriteria(int level, Query q){
        return buildNonBQCriteria(level, SHOULD, q);
    }

    public static List<TermCriteria> buildNonBQCriteria(int level, BooleanClause bc, Query q){
        return buildNonBQCriteria(level, bc.getOccur(), q);
    }

    public static List<TermCriteria> buildNonBQCriteria(int level, Occur occur, Query q){
        logger.debug(String.format("%s%s %s  {%s}", getPrintPrefix(level), occur.name(), q.getClass().getSimpleName(), q));
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
            tcList.add(new TermCriteria(t.text(), t.field()));
        }
        return tcList;
    }

    private static final String prettyPrintPrefix = " ----- ";

    private static String getPrintPrefix(int level){
        String printPrefix = "";
        for(int i =1; i < level; i++) printPrefix += prettyPrintPrefix;
        return  String.format(" [%d] %s > ", level, printPrefix);
    }

}
