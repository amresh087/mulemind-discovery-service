package com.mulemind.discovery.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.mulemind.discovery.entity.TransactionType;

@Repository
public interface TransactionTypeRepository extends JpaRepository<TransactionType, String> {
}
