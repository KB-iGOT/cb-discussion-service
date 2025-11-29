package com.igot.cb.pores.elasticsearch.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class SearchResult implements Serializable {

  private static final long serialVersionUID = 1L;

  private transient List<Map<String, Object>> data;
  private Map<String, List<FacetDTO>> facets;
  private long totalCount;
}
