package com.jarvis.core.criteria;


import java.util.HashMap;
import java.util.Map;

public class TermCriteriaMap {

    private Map<String, TermCriteria> criteriaMap = new HashMap<String, TermCriteria>();

    public TermCriteriaMap(){}

    public void addRootCriteria(String term, TermCriteria termCriteria){
        if(term == null || termCriteria == null)
            throw new IllegalArgumentException("Cannot add NULL value as key or value");
        if(criteriaMap.containsKey(term)) {
            throw new IllegalStateException("Criteria already exists for this key. Remove existing key before attempting to add.");
        } else {
          criteriaMap.put(term, termCriteria);
        }
    }

}
