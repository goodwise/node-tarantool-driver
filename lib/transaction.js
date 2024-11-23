class Transaction {
    constructor (self) {
        Object.assign(
            this, 
            {
                streamId: self._getRequestId()
            },
            self
        )
    };
    // async begin (transactionTimeout, isolationLevel, opts) {

    //     return this._begin(transactionTimeout, isolationLevel, opts);
    // };
    // async commit () {
    //     return this._commit();
    // };
    // async rollback () {
    //     return this._rollback();
    // };
};

function createTransaction () {
    return new Transaction(this);
}

Transaction.prototype.transaction = createTransaction

module.exports = Transaction