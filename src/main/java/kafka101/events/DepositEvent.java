package kafka101.events;

public class DepositEvent extends Event{
    public DepositEvent(int userID, int amount){
        this.userID = userID;
        this.amount = amount;
        this.eventType = EventType.DEPOSIT;
    }
}
