package com.mulemind.discovery.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "transaction_type")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionType {

    @Id
    @Column(name = "transaction_code", nullable = false, length = 20)
    private String transactionCode;

    @Column(name = "document_name", nullable = false)
    private String documentName;

    @Column(name = "purpose", nullable = false, length = 1000)
    private String purpose;
}
