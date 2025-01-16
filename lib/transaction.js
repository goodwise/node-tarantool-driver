class Transaction {
    constructor (self) {
        this.streamId = self._getRequestId();
        Object.assign(
            this,
            self
        )
    };
};

function createTransaction () {
    return new Transaction(this);
}

Transaction.prototype.transaction = createTransaction

module.exports = Transaction