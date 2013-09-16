<?php
namespace Bread\Storage\Drivers\LDAP;
use stdClass as StdClass;

/**
 * This abstract class provides the basic variables and methods.
 *
 * @package phpLDAPadmin
 * @subpackage DataStore
 */
abstract class DS {
    # ID of this db.
    protected $index;

    # Configuration paramters.
    protected $default;
    protected $custom;
    protected $type;

    abstract function __construct($index);

    /**
     * This will make the connection to the datasource
    */
    abstract protected function connect($method,$debug=false);

    /**
     * Login to the datastore
     *  method: default = anon, connect to ds using bind_id not auth_id.
     *  method: 'user', connect with auth_id
     *  method: '<freetext>', any custom extra connection to ds.
    */
    abstract public function login($user=null,$pass=null,$method=null);

    /**
     * Query the datasource
    */
    abstract public function query($query,$method,$index=null,$debug=false);

    /**
     * Return error details from previous operation
    */
    abstract protected function getErrorMessage();
    abstract protected function getErrorNum();

    /**
     * Functions that set and verify object configuration details
    */
    public function setDefaults($defaults) {
        foreach ($defaults as $key => $details)
        foreach ($details as $setting => $value)
            $this->default->{$key}[$setting] = $value;
    }

    public function isDefaultKey($key) {
        return isset($this->default->$key);
    }

    public function isDefaultSetting($key,$setting) {
        return array_key_exists($setting,$this->default->{$key});
    }

    /**
     * Return a configuration value
     */
    public function getValue($key,$setting,$fatal=true) {
        if (defined('DEBUG_ENABLED') && DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',17,1,__FILE__,__LINE__,__METHOD__,$fargs);

        if (isset($this->custom->{$key}[$setting]))
            return $this->custom->{$key}[$setting];

        elseif (isset($this->default->{$key}[$setting]) && array_key_exists('default',$this->default->{$key}[$setting]))
        return $this->default->{$key}[$setting]['default'];

        elseif ($fatal)
        debug_dump_backtrace("Error trying to get a non-existant value ($key,$setting)",1);

        else
            return null;
    }

    /**
     * Set a configuration value
     */
    public function setValue($key,$setting,$value) {
        if (isset($this->custom->{$key}[$setting]))
            system_message(array(
                'title'=>_('Configuration setting already defined.'),
                'body'=>sprintf('A call has been made to reset a configuration value (%s,%s,%s)',
                    $key,$setting,$value),
                'type'=>'info'));

        $this->custom->{$key}[$setting] = $value;
    }

    /**
     * Return the untested config items
     */
    public function untested() {
        $result = array();

        foreach ($this->default as $option => $details)
        foreach ($details as $param => $values)
        if (isset($values['untested']) && $values['untested'])
            array_push($result,sprintf('%s.%s',$option,$param));

        return $result;
    }

    /**
     * Get the name of this datastore
     */
    public function getName() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',17,0,__FILE__,__LINE__,__METHOD__,$fargs);

        return $this->getValue('server','name');
    }

    /**
     * Functions that enable login and logout of the application
     */
    /**
     * Return the authentication type for this object
     */
    public function getAuthType() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',17,0,__FILE__,__LINE__,__METHOD__,$fargs);

        switch ($this->getValue('login','auth_type')) {
          case 'cookie':
          case 'config':
          case 'http':
          case 'proxy':
          case 'session':
          case 'sasl':
              return $this->getValue('login','auth_type');

          default:
              die(sprintf('Error: <b>%s</b> hasnt been configured for auth_type <b>%s</b>',__METHOD__,
              $this->getValue('login','auth_type')));
        }
    }

    /**
     * Get the login name of the user logged into this datastore's connection method
     * If this returns null, we are not logged in.
     * If this returns '', we are logged in with anonymous
     */
    public function getLogin($method=null) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',17,0,__FILE__,__LINE__,__METHOD__,$fargs);

        $method = $this->getMethod($method);

        # For anonymous binds
        if ($method == 'anon')
        if (isset($_SESSION['USER'][$this->index][$method]['name']))
            return '';
        else
            return null;

        switch ($this->getAuthType()) {
          case 'cookie':
              if (! isset($_COOKIE[$method.'-USER']))
                  # If our bind_id is set, we'll pass that back for logins.
                  return (! is_null($this->getValue('login','bind_id')) && $method == 'login') ? $this->getValue('login','bind_id') : null;
              else
                  return blowfish_decrypt($_COOKIE[$method.'-USER']);

          case 'config':
              if (! isset($_SESSION['USER'][$this->index][$method]['name']))
                  return $this->getValue('login','bind_id');
              else
                  return blowfish_decrypt($_SESSION['USER'][$this->index][$method]['name']);

          case 'proxy':
              if (! isset($_SESSION['USER'][$this->index][$method]['proxy']))
                  return $this->getValue('login','bind_id');
              else
                  return blowfish_decrypt($_SESSION['USER'][$this->index][$method]['proxy']);

          case 'http':
          case 'session':
          case 'sasl':
              if (! isset($_SESSION['USER'][$this->index][$method]['name']))
                  # If our bind_id is set, we'll pass that back for logins.
                  return (! is_null($this->getValue('login','bind_id')) && $method == 'login') ? $this->getValue('login','bind_id') : null;
              else
                  return blowfish_decrypt($_SESSION['USER'][$this->index][$method]['name']);

          default:
              die(sprintf('Error: %s hasnt been configured for auth_type %s',__METHOD__,$this->getAuthType()));
        }
    }

    /**
     * Set the login details of the user logged into this datastore's connection method
     */
    protected function setLogin($user,$pass,$method=null) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',17,0,__FILE__,__LINE__,__METHOD__,$fargs);

        $method = $this->getMethod($method);

        switch ($this->getAuthType()) {
          case 'cookie':
              set_cookie($method.'-USER',blowfish_encrypt($user),NULL,'/');
              set_cookie($method.'-PASS',blowfish_encrypt($pass),NULL,'/');
              return true;

          case 'config':
              return true;

          case 'proxy':
              if (isset($_SESSION['USER'][$this->index][$method]['proxy']))
                  unset($_SESSION['USER'][$this->index][$method]['proxy']);

          case 'http':
          case 'session':
          case 'sasl':
              $_SESSION['USER'][$this->index][$method]['name'] = blowfish_encrypt($user);
              $_SESSION['USER'][$this->index][$method]['pass'] = blowfish_encrypt($pass);

              return true;

          default:
              die(sprintf('Error: %s hasnt been configured for auth_type %s',__METHOD__,$this->getAuthType()));
        }
    }

    /**
     * Get the login password of the user logged into this datastore's connection method
     */
    protected function getPassword($method=null) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',17,0,__FILE__,__LINE__,__METHOD__,$fargs);

        $method = $this->getMethod($method);

        # For anonymous binds
        if ($method == 'anon')
        if (isset($_SESSION['USER'][$this->index][$method]['name']))
            return '';
        else
            return null;

        switch ($this->getAuthType()) {
          case 'cookie':
              if (! isset($_COOKIE[$method.'-PASS']))
                  # If our bind_id is set, we'll pass that back for logins.
                  return (! is_null($this->getValue('login','bind_pass')) && $method == 'login') ? $this->getValue('login','bind_pass') : null;
              else
                  return blowfish_decrypt($_COOKIE[$method.'-PASS']);

          case 'config':
          case 'proxy':
              if (! isset($_SESSION['USER'][$this->index][$method]['pass']))
                  return $this->getValue('login','bind_pass');
              else
                  return blowfish_decrypt($_SESSION['USER'][$this->index][$method]['pass']);

          case 'http':
          case 'session':
          case 'sasl':
              if (! isset($_SESSION['USER'][$this->index][$method]['pass']))
                  # If our bind_pass is set, we'll pass that back for logins.
                  return (! is_null($this->getValue('login','bind_pass')) && $method == 'login') ? $this->getValue('login','bind_pass') : null;
              else
                  return blowfish_decrypt($_SESSION['USER'][$this->index][$method]['pass']);

          default:
              die(sprintf('Error: %s hasnt been configured for auth_type %s',__METHOD__,$this->getAuthType()));
        }
    }

    /**
     * Return if this datastore's connection method has been logged into
     */
    public function isLoggedIn($method=null) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',17,0,__FILE__,__LINE__,__METHOD__,$fargs);

        static $CACHE = array();

        $method = $this->getMethod($method);

        if (isset($CACHE[$this->index][$method]) && ! is_null($CACHE[$this->index][$method]))
            return $CACHE[$this->index][$method];

        $CACHE[$this->index][$method] = null;

        # For some authentication types, we need to do the login here
        switch ($this->getAuthType()) {
          case 'config':
              if (! $CACHE[$this->index][$method] = $this->login($this->getLogin($method),$this->getPassword($method),$method))
                  system_message(array(
                      'title'=>_('Unable to login.'),
                      'body'=>_('Your configuration file has authentication set to CONFIG based authentication, however, the userid/password failed to login'),
                      'type'=>'error'));

              break;

          case 'http':
              # If our auth vars are not set, throw up a login box.
              if (! isset($_SERVER['PHP_AUTH_USER'])) {
                  # If this server is not in focus, skip the basic auth prompt.
                  if (get_request('server_id','REQUEST') != $this->getIndex()) {
                      $CACHE[$this->index][$method] = false;
                      break;
                  }

                  header(sprintf('WWW-Authenticate: Basic realm="%s %s"',app_name(),_('login')));

                  if ($_SERVER['SERVER_PROTOCOL'] == 'HTTP/1.0')
                      header('HTTP/1.0 401 Unauthorized'); // http 1.0 method
                  else
                      header('Status: 401 Unauthorized'); // http 1.1 method

                  # If we still dont have login details...
                  if (! isset($_SERVER['PHP_AUTH_USER'])) {
                      system_message(array(
                      'title'=>_('Unable to login.'),
                      'body'=>_('Your configuration file has authentication set to HTTP based authentication, however, there was none presented'),
                      'type'=>'error'));

                      $CACHE[$this->index][$method] = false;
                  }

                  # Check our auth vars are valid.
                  } else {
					if (! $this->login($_SERVER['PHP_AUTH_USER'],$_SERVER['PHP_AUTH_PW'],$method)) {
					    system_message(array(
					    'title'=>_('Unable to login.'),
							'body'=>_('Your HTTP based authentication is not accepted by the LDAP server'),
							'type'=>'error'));

                  $CACHE[$this->index][$method] = false;

                  } else
                      $CACHE[$this->index][$method] = true;
        }

        break;

			case 'proxy':
			$CACHE[$this->index][$method] = $this->login($this->getValue('login','bind_id'),$this->getValue('login','bind_pass'),$method);

			break;

			case 'sasl':
				# Propogate any given Kerberos credential cache location
				if (isset($_ENV['REDIRECT_KRB5CCNAME']))
				putenv(sprintf('KRB5CCNAME=%s',$_ENV['REDIRECT_KRB5CCNAME']));
				elseif (isset($_SERVER['KRB5CCNAME']))
			putenv(sprintf('KRB5CCNAME=%s',$_SERVER['KRB5CCNAME']));

			# Map the SASL auth ID to a DN
			$regex = $this->getValue('login', 'sasl_dn_regex');
			$replacement = $this->getValue('login', 'sasl_dn_replacement');

			if ($regex && $replacement) {
			$userDN = preg_replace($regex, $replacement, $_SERVER['REMOTE_USER']);

			$CACHE[$this->index][$method] = $this->login($userDN, '', $method);

			# Otherwise, use the user name as is
			# For GSSAPI Authentication + mod_auth_kerb and Basic Authentication
				} else
				$CACHE[$this->index][$method] = $this->login(isset($_SERVER['REMOTE_USER']) ? $_SERVER['REMOTE_USER'] : '', '', $method);

				break;

				default:
				$CACHE[$this->index][$method] = is_null($this->getLogin($method)) ? false : true;
				}

				return $CACHE[$this->index][$method];
}

/**
* Logout of this datastore's connection method
*/
public function logout($method=null) {
if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
debug_log('Entered (%%)',17,0,__FILE__,__LINE__,__METHOD__,$fargs);

$method = $this->getMethod($method);

unset ($_SESSION['cache'][$this->index]);

switch ($this->getAuthType()) {
case 'cookie':
set_cookie($method.'-USER','',time()-3600,'/');
set_cookie($method.'-PASS','',time()-3600,'/');

case 'config':
return true;

    case 'http':
    case 'proxy':
    case 'session':
    case 'sasl':
    if (isset($_SESSION['USER'][$this->index][$method]))
        unset($_SESSION['USER'][$this->index][$method]);

				return true;

			default:
				die(sprintf('Error: %s hasnt been configured for auth_type %s',__METHOD__,$this->getAuthType()));
    }
    }

    /**
    * Functions that return the condition of the datasource
    */
    public function isVisible() {
    if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
    debug_log('Entered (%%)',17,0,__FILE__,__LINE__,__METHOD__,$fargs);

    return $this->getValue('server','visible');
    }

    public function isReadOnly() {
    if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
			    debug_log('Entered (%%)',17,0,__FILE__,__LINE__,__METHOD__,$fargs);

		if (! trim($this->getLogin(null)) && $_SESSION[APPCONFIG]->getValue('appearance','anonymous_bind_implies_read_only'))
		return true;
		else
			    return $this->getValue('server','read_only');
			}

			public function getIndex() {
		if (defined('DEBUG_ENABLED') && DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
		debug_log('Entered (%%)',17,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->index);

			return $this->index;
			}

			/**
			* Work out which connection method to use.
			* If a method is passed, then it will be passed back. If no method is passed, then we'll
			* check to see if the user is logged in. If they are, then 'user' is used, otherwise
			* 'anon' is used.
			*
			* @param int Server ID
			* @return string Connection Method
			*/
			protected function getMethod($method=null) {
			if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
			debug_log('Entered (%%)',17,0,__FILE__,__LINE__,__METHOD__,$fargs);

			static $CACHE = array();

			# Immediately return if method is set.
			if (! is_null($method))
			    return $method;

			    # If we have been here already, then return our result
		if (isset($CACHE[$this->index]) && ! is_null($CACHE))
		return $CACHE[$this->index];

		$CACHE[$this->index] = 'anon';

			if ($this->isLoggedIn('user'))
			    $CACHE[$this->index] = 'user';

			    return $CACHE[$this->index];
			    }

			    /**
			    * This method should be overridden in application specific ds files
			    */
			    public function isSessionValid() {
			    if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
			    debug_log('Entered (%%)',17,1,__FILE__,__LINE__,__METHOD__,$fargs,true);

			    return true;
			    }

			    /**
			        * Return the time left in seconds until this connection times out. If there is not timeout,
			        * this function will return null.
			        */
			        public function inactivityTime() {
		if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
			debug_log('Entered (%%)',17,0,__FILE__,__LINE__,__METHOD__,$fargs);

		if ($this->isLoggedIn() && ! in_array($this->getAuthType(),array('config','http')))
			return time()+($this->getValue('login','timeout')*60);
		else
			return null;
	}
}
