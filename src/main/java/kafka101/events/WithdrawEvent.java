package kafka101.events;

public class WithdrawEvent extends Event{
    public WithdrawEvent(int userID, int amount){
        this.userID = userID;
        this.amount = amount;
        this.eventType = EventType.WITHDRAW;
    }
}
