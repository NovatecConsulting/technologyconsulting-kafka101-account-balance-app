package kafka101.controller;

import kafka101.service.AccountService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {
    @Autowired
    AccountService accountService;

    @RequestMapping("/deposit/{id}/{amount}")
    public String produceDepositEvent(@PathVariable("id") int id, @PathVariable("amount") int amount){
        return accountService.depositService(id, amount);
    }

    @RequestMapping("/withdraw/{id}/{amount}")
    public String produceWithDrawEvent(@PathVariable("id") int id, @PathVariable("amount") int amount){
        return accountService.withdrawService(id, amount);
    }

    @RequestMapping("/history")
    public String getTransaction() {
        return accountService.getTransaction();
    }

    @RequestMapping("/balance/{id}")
    public String getBalance(@PathVariable("id") int id){
        return accountService.getBalance(id);
    }

    @RequestMapping("/balance")
    public String getAllBalances(){
        return accountService.getAllBalances();
    }
}
