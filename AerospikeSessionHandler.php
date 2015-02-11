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
		$options = array(
			Aerospike::OPT_POLICY_EXISTS => Aerospike::POLICY_EXISTS_CREATE,
		);
		do{
			$session_id = mt_rand(0, 0xff) << 56 | mt_rand(0, 0xfffffff) << 28 | mt_rand(0, 0xfffffff);
			$key = $this->_db->initKey($this->_namespace, $this->_set, $session_id);
			$bins = array(
				'created_at' => time(),
			);
			$status = $this->_db->put($key, $bins, $this->_ttl, $options);
		}
		while($status === Aerospike::ERR_RECORD_EXISTS);

		$this->_session_data[$session_id] = msgpack_pack($bins);

		return sprintf('%016x', $session_id);
	}

	public function destroy($session_id) {
		$session_id = hexdec(substr($session_id, 0, 8)) << 32 | hexdec(substr($session_id, 8, 8));
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
		if (!preg_match('/^[0-9a-f]{16}$/', $session_id))
			throw new Duoshuo\Exception('不合法的session id', Duoshuo\Exception::MISSING_OR_INVALID_ARGUMENT);

		$session_id = hexdec(substr($session_id, 0, 8)) << 32 | hexdec(substr($session_id, 8, 8));

		if (isset($this->_session_data[$session_id]))
			return $this->_session_data[$session_id];

		$key = $this->_db->initKey($this->_namespace, $this->_set, $session_id);
		$record = array();
		$status = $this->_db->get($key, $record);

		if ($status === Aerospike::OK) {
			$session_data_array = $record['bins'];
		}
		else {
			$session_data_array = array(
				'created_at' => time(),
			);
			$this->_db->put($key, $session_data_array, $this->_ttl);
		}

		return $this->_session_data[$session_id] = msgpack_pack($session_data_array);
	}

	public function write($session_id, $session_data) {
		$session_id = hexdec(substr($session_id, 0, 8)) << 32 | hexdec(substr($session_id, 8, 8));
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
