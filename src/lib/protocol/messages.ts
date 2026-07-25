/** Wire tag for an actor living on a worker. */
export type ActorRef = {
	type: "actor_ref";
	workerId: number;
	objectId: number;
};

export type BridgeCallMessage = {
	bridge: "call";
	id: number;
	targetWorkerId: number;
	objectId: number;
	method: string;
	args: unknown[];
};

export type BridgeResultMessage =
	| {
		bridge: "result";
		id: number;
		result: unknown;
	}
	| {
		bridge: "result";
		id: number;
		error: {
			message: string;
			name?: string;
			stack?: string;
		};
	};

export type ProtocolMessage =
	| {
		command: "register";
		id: number;
		className: string;
		moduleUrl: string;
		exportName: string;
	}
	| {
		command: "create";
		id: number;
		objectId: number;
		className: string;
		args: unknown[];
	}
	| {
		command: "call";
		id: number;
		objectId: number;
		method: string;
		args: unknown[];
	}
	| {
		command: "destroy";
		id: number;
		objectId: number;
	}
	| {
		command: "close_all";
		id: number;
	};

export type ProtocolResponse =
	| {
		id: number;
		result: unknown;
	}
	| {
		id: number;
		error: {
			message: string;
			name?: string;
			stack?: string;
		};
	};
