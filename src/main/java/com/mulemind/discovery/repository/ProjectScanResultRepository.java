package com.mulemind.discovery.repository;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.mulemind.discovery.entity.ProjectScanResult;

@Repository
public interface ProjectScanResultRepository extends JpaRepository<ProjectScanResult, UUID> {
}
