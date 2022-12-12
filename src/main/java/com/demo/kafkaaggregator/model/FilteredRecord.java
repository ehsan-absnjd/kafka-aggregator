package com.demo.kafkaaggregator.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.Date;

@Data
@Builder
@Document(indexName = "urls")
@NoArgsConstructor
@AllArgsConstructor
public class FilteredRecord {
    @Id
    private String id;

    @Field(type = FieldType.Text, name = "url")
    private String url;

    @Field(type = FieldType.Integer, name = "count")
    private long count;

    @Field(type = FieldType.Text, name = "producer")
    private String producer;

    @Field(type = FieldType.Date, name = "date")
    private Date date;
}
