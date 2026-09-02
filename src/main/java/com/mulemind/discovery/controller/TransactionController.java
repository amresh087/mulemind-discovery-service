package com.mulemind.discovery.controller;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
@RequestMapping("/transaction")
public class TransactionController {

    //private final TransactionService transactionService;


    @DeleteMapping("/transaction-types/{transactionCode}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteTransactionType(@PathVariable String transactionCode) {
        //transactionService.deleteTransactionType(transactionCode);
    }
}
