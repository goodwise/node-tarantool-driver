function findPipelineError (array = []) {
    var error = (array || this).find(element => element[0])
    return error[0] ?? null;
};

function findPipelineErrors (array = []) {
    var aoe = [];
    for (var subarray of (array || this)) {
        var errored_element = subarray[0]
        if (errored_element) aoe.push(errored_element)
    }

    return aoe;
};

class PipelineResponse extends Array {
    findPipelineError = findPipelineError;
    findPipelineErrors = findPipelineErrors;
    static findPipelineError = findPipelineError;
    static findPipelineErrors = findPipelineErrors;

    constructor (arr) {
        super(...arr)
    };

	get pipelineError () {
		return this.findPipelineError()
	};

	get pipelineErrors () {
		return this.findPipelineErrors()
	}
}
module.exports = PipelineResponse;