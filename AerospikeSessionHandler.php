<?php
class AerospikeSessionHandler implements \SessionHandlerInterface {
	protected $_db = null;

	protected $_namespace;

	protected $_set;

	protected $_ttl;

	protected $_session_data = [];

	public function __construct($db, $namespace, $set, $ttl = 1440) {
		$this->_db = $db;
		$this->_namespace = $namespace;
		$this->_set = $set;
		$this->_ttl = $ttl;
	}

	public function close() {
		return true;
	}

	public function create_sid() {
		do{
			$session_id = mt_rand(0, PHP_INT_MAX);
			$key = $this->_db->initKey($this->_namespace, $this->_set, $session_id);
			$metadata = array();
			$status = $this->_db->exists($key, $metadata);
		}
		while($status === Aerospike::OK);

		$this->_session_data[$session_id] = '';

		return (string)$session_id;
	}

	public function destroy($session_id) {
		$key = $this->_db->initKey($this->_namespace, $this->_set, $session_id);
		$this->_db->remove($key);

		return true;
	}

	public function gc($maxlifetime) {
		return true;
	}

	public function open($save_path, $name) {
		return true;
	}

	public function read($session_id) {
		if (isset($this->_session_data[$session_id]))
			return $this->_session_data[$session_id];

		$key = $this->_db->initKey($this->_namespace, $this->_set, $session_id);
		$record = array();
		$status = $this->_db->get($key, $record);

		$session_data_array = $status === Aerospike::OK ? $record['bins'] : array();

		return $this->_session_data[$session_id] = msgpack_pack($session_data_array);
	}

	public function write($session_id, $session_data) {
		$key = $this->_db->initKey($this->_namespace, $this->_set, $session_id);

		if (isset($this->_session_data[$session_id]) && $this->_session_data[$session_id] === $session_data) {
			$this->_db->touch($key, $this->_ttl);
		}
		else {
			$this->_session_data[$session_id] = $session_data;

			$bins = msgpack_unpack($session_data);
			$options = array(
				Aerospike::OPT_POLICY_EXISTS => Aerospike::POLICY_EXISTS_CREATE_OR_REPLACE,
			);
			$this->_db->put($key, $bins, $this->_ttl, $options);
		}

		return true;
	}
}
