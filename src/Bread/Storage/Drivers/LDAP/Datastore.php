<?php
namespace Bread\Storage\Drivers\LDAP;
use stdClass as StdClass;


/**
 * The list of database sources
 *
 * @package phpLDAPadmin
 * @subpackage DataStore
 */
class Datastore {
	# Out DS index id
	private $index;
	# List of all the objects
	private $objects = array();
	# Default settings
	private $default;

	public function __construct() {
		$this->default = new StdClass;

		$this->default->server['id'] = array(
			'desc'=>'Server ID',
			'default'=>null);

		$this->default->server['name'] = array(
			'desc'=>'Server name',
			'default'=>null);

		# Connectivity Info
		$this->default->server['host'] = array(
			'desc'=>'Host Name',
			'default'=>'127.0.0.1');

		$this->default->server['port'] = array(
			'desc'=>'Port Number',
			'default'=>null);

		# Read or write only access
		$this->default->server['read_only'] = array(
			'desc'=>'Server is in READ ONLY mode',
			'default'=>false);

		$this->default->server['visible'] = array(
			'desc'=>'Whether this server is visible',
			'default'=>true);

		$this->default->server['hide_noaccess_base'] = array(
			'desc'=>'If base DNs are not accessible, hide them instead of showing create',
			'default'=>false);

		# Authentication Information
		$this->default->login['auth_type'] = array(
			'desc'=>'Authentication Type',
			'default'=>'session');

/*
		/* ID to login to this application, this assumes that there is
		 * application authentication on top of authentication required to
		 * access the data source **
		$this->default->login['auth_id'] = array(
			'desc'=>'User Login ID to login to this DS',
			'untested'=>true,
			'default'=>null);

		$this->default->login['auth_pass'] = array(
			'desc'=>'User Login Password to login to this DS',
			'untested'=>true,
			'default'=>null);
*/

		$this->default->login['auth_text'] = array(
			'desc'=>'Text to show at the login prompt',
			'default'=>null);

		$this->default->login['bind_id'] = array(
			'desc'=>'User Login ID to bind to this DS',
			'default'=>null);

		$this->default->login['bind_pass'] = array(
			'desc'=>'User Login Password to bind to this DS',
			'default'=>null);

		$this->default->login['timeout'] = array(
			'desc'=>'Session timout in seconds',
			'default'=>session_cache_expire()-1);

		$this->default->login['sasl_dn_regex'] = array(
			'desc'=>'SASL authorization id to user dn PCRE regular expression',
			'untested'=>true,
			'default'=>null);

		$this->default->login['sasl_dn_replacement'] = array(
			'desc'=>'SASL authorization id to user dn PCRE regular expression replacement string',
			'untested'=>true,
			'default'=>null);

		# Prefix for custom pages
		$this->default->custom['pages_prefix'] = array(
			'desc'=>'Prefix name for custom pages',
			'default'=>'custom_');
	}

	/**
	 * Create a new database object
	 */
	public function newServer($type) {
		if (class_exists($type)) {
			$this->index = count($this->objects)+1;
			$this->objects[$this->index] = new $type($this->index);

			$this->objects[$this->index]->setDefaults($this->default);
			return $this->index;

		} else {
			printf('ERROR: Class [%s] doesnt exist',$type);
			die();
		}
	}

	/**
	 * Set values for a database object.
	 */
	public function setValue($key,$setting,$value) {
		if (! $this->objects[$this->index]->isDefaultKey($key))
			error("ERROR: Setting a key [$key] that isnt predefined.",'error',true);

		if (! $this->objects[$this->index]->isDefaultSetting($key,$setting))
			error("ERROR: Setting a index [$key,$setting] that isnt predefined.",'error',true);

		# Test if its should be an array or not.
		if (is_array($this->objects[$this->index]->getValue($key,$setting)) && ! is_array($value))
			error("Error in configuration file, {$key}['$setting'] SHOULD be an array of values.",'error',true);

		if (! is_array($this->objects[$this->index]->getValue($key,$setting)) && is_array($value))
			error("Error in configuration file, {$key}['$setting'] should NOT be an array of values.",'error',true);

		# Store the value in the object.
		$this->objects[$this->index]->setValue($key,$setting,$value);
	}

	/**
	 * Get a list of all the configured servers.
	 *
	 * @param boolean Only show visible servers.
	 * @return array list of all configured servers.
	 */
	public function getServerList($isVisible=true) {
		if (defined('DEBUG_ENABLED') && DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
			debug_log('Entered (%%)',17,0,__FILE__,__LINE__,__METHOD__,$fargs);

		static $CACHE;

		if (isset($CACHE[$isVisible]))
			return $CACHE[$isVisible];

		$CACHE[$isVisible] = array();

		# Debugging incase objects is not set.
		if (! $this->objects) {
			print "<PRE>";
			debug_print_backtrace();
			die();
		}

		foreach ($this->objects as $id => $server)
			if (! $isVisible || ($isVisible && $server->getValue('server','visible')))
				$CACHE[$isVisible][$id] = $server;

		masort($CACHE[$isVisible],'name');

		return $CACHE[$isVisible];
	}

	/**
	 * Return an object Instance of a configured database.
	 *
	 * @param int Index
	 * @return object Datastore instance object.
	 */
	public function Instance($index=null) {
		if (defined('DEBUG_ENABLED') && DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
			debug_log('Entered (%%)',17,0,__FILE__,__LINE__,__METHOD__,$fargs);

		# If no index defined, then pick the lowest one.
		if (is_null($index) || ! trim($index) || ! is_numeric($index))
			$index = min($this->GetServerList())->getIndex();

		if (! isset($this->objects[$index]))
			debug_dump_backtrace(sprintf('Error: Datastore instance [%s] doesnt exist?',htmlspecialchars($index)),1);

		if (defined('DEBUG_ENABLED') && DEBUG_ENABLED)
			debug_log('Returning instance of database (%s)',3,0,__FILE__,__LINE__,__METHOD__,$index);

		return $this->objects[$index];
	}

	/**
	 * Return an object Instance of a configured database.
	 *
	 * @param string Name of the instance to retrieve
	 * @return object Datastore instance object.
	 */
	public function InstanceName($name=null) {
		if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
			debug_log('Entered (%%)',17,0,__FILE__,__LINE__,__METHOD__,$fargs);

		foreach ($this->getServerList(false) as $index)
			if ($this->objects[$index]->getName() == $name)
				return $this->objects[$index];

		# If we get here, then no object with the name exists.
		return null;
	}

	/**
	 * Return an object Instance of a configured database.
	 *
	 * @param string ID of the instance to retrieve
	 * @return object Datastore instance object.
	 */
	public function InstanceId($id=null) {
		if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
			debug_log('Entered (%%)',17,0,__FILE__,__LINE__,__METHOD__,$fargs);

		foreach ($this->getServerList(false) as $index)
			if ($this->objects[$index->getIndex()]->getValue('server','id') == $id)
				return $this->objects[$index->getIndex()];

		# If we get here, then no object with the name exists.
		return null;
	}
}
