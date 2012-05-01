<?php
/**
 * @package ActiveRecord
 */
namespace ActiveRecord;

/**
 * Adapter for Google Fusion Tables (work in progress)
 * Uses code from http://code.google.com/p/fusion-tables-client-php/ (class ClientLogin and FTClientLogin)
 *
 * @package ActiveRecord
 * @author Martijn Durnez
 */
class FusionAdapter extends Connection
{
	private static $token;
	private $table;
	private $called_class;
	private $last_result;
	
	public function __construct($info) 
	{
		self::$token = ClientLogin::getAuthToken($info->user, $info->pass);
		$this->connection = new FTClientLogin(self::$token);
		Model::$primary_key = 'rowid';
	}
	
	public function query($sql, &$values=array()) 
	{
		$this->last_query = $sql;
		if(!empty($values)) {
			//Fusion Tables doesn't accept parameterized query
			foreach($values as $val) {
				$val = "'$val'";
				$sql = substr_replace($sql, " ".$val . " ", strpos($sql, '?'), 1);
			}
		}
		//fusion tables doesn't include the primary_key (rowid) in queries if not specified.Need it for updates so we 			
		//add it anyway
		if(is_numeric(stripos($sql, 'SELECT * FROM'))) {
			$cols = $this->columns($this->table);
			$select = implode('`, `', array_keys($cols));
			$select = "SELECT `" . $select . "`";
			$sql = str_replace('SELECT *', $select, $sql);
		} elseif(is_numeric(stripos($sql, 'SELECT'))) {
			$match = explode('FROM', $sql);
			$select = $match[0];
			if(!is_numeric(stripos($select, 'rowid'))) {
				$select = $select . ', rowid ';
				$sql = str_replace($match[0], $select, $sql);
			}
			
		}
		$result = $this->connection->query($sql);
		//var_dump($sql);
		$result = new FusionResult($result);
		//log the last result to get the insert_id if there's an insert
		$this->last_result = $result;
		return $result;
	}

	public function limit($sql, $offset, $limit)
	{
		$offset = $offset ? $offset : 0;
		return "$sql OFFSET $offset LIMIT $limit";
	}
	
	public function columns($table)
	{
		$this->table = $table;
		if($columns = self::get_cache_value($table)) {
			return $columns;
		}	
		$columns = array();
		$sth = $this->query_column_info($table);
		foreach($sth as $row) {
			$c = $this->create_column($row);
			$columns[$c->name] = $c;
		}
		//add the rowid column because Fusion Tables doesn't include it by default
		$row = array('name' => 'rowid', 'type' => 'integer', 'pk' => true, 'auto_increment' => true);
		$columns['rowid'] = $this->create_column($row);
		self::set_cache($table, $columns);
		return $columns;
	}
	
	public function query_column_info($table)
	{	
		$this->table = $table;
		return $this->query("DESCRIBE $table");
	}
	
	public function insert_id($sequence=null)
	{
		$result = $this->last_result->fetch();
		return (int) $result['rowid'];
		
	}
	
	public function create_column(&$column)
	{
		$c = new Column();
		$c->name = isset($column['column id']) ? $column['column id'] : 'rowid';
		$coltype = $column['type'] == 'number' ? 'integer' : $column['type'];
		$c->pk = isset($column['pk']);
		$c->auto_increment = isset($column['auto_increment']);
		$c->raw_type = $coltype;
		$c->type = $coltype;
		$c->inflected_name = Inflector::instance()->variablize($column['name']);
		$c->colid = isset($column['column id']) ? $column['column id'] : 'rowid';
		$c->map_raw_type();
		return $c;
	
	}
	
	public function query_for_tables()
	{
		return $this->query('SHOW TABLES');
	}
	
		
	public function set_encoding($charset)
	{
		return false;
	}
	
	public function native_database_types()
	{
		return array(
			'string' => array('name' => 'string', 'length' => 255),
			'integer' => array('name' => 'number', 'length' => 11)
		);
	}
	
	/**
	 * cache some data in a SESSIOn so we don't have to make a trip to Google Fusion Tables
	 * 
	 */
	
	protected static function set_cache($key,$value) 
	{
		$class = get_called_class();
		$_SESSION[$class][$key] = serialize($value);
	}
	
	protected static function get_cache_value($key) 
	{
		$class = get_called_class();
		if(self::cache_exists($key)) {
			return unserialize($_SESSION[$class][$key]);
		} else {
			return false;
		}
		
	}
	
	protected static function cache_exists($key) 
	{
		$class = get_called_class();
		return isset($_SESSION[$class][$key]);
	}

}


define('URL', 'https://www.google.com/fusiontables/api/query');
define('SCOPE', 'https://www.googleapis.com/auth/fusiontables');
define('SERVER_URI', 'https://www.google.com');
define('GOOGLE_OAUTH_REQUEST_TOKEN_API', 'https://www.google.com/accounts/OAuthGetRequestToken');
define('GOOGLE_OAUTH_ACCESS_TOKEN_API', 'https://www.google.com/accounts/OAuthGetAccessToken');
define('GOOGLE_OAUTH_AUTHORIZE_API', 'https://www.google.com/accounts/OAuthAuthorizeToken');

class ClientLogin extends FusionAdapter {
	public static function getAuthToken($username, $password) 
	{
		if($token = self::get_cache_value('token')) {
			return $token;
		}
		$clientlogin_curl = curl_init();
		curl_setopt($clientlogin_curl,CURLOPT_URL,'https://www.google.com/accounts/ClientLogin');
		curl_setopt($clientlogin_curl, CURLOPT_POST, true);
		curl_setopt ($clientlogin_curl, CURLOPT_POSTFIELDS,
		"Email=".$username."&Passwd=".$password."&service=fusiontables&accountType=GOOGLE");
		curl_setopt($clientlogin_curl,CURLOPT_CONNECTTIMEOUT,2);
		curl_setopt($clientlogin_curl,CURLOPT_RETURNTRANSFER,1);
		$token = curl_exec($clientlogin_curl);
		curl_close($clientlogin_curl);
		$token_array = explode("=", $token);
		$token = str_replace("\n", "", $token_array[3]);
		self::set_cache('token', $token);
		return $token;
	}
}


class FTClientLogin {
	function __construct($token) 
	{
		$this->token = $token;
		
	}
	
	function query($query, $gsessionid = null, $recursedOnce = false) 
	{

		$url = URL;
		$query =  "sql=".urlencode($query);

		$fusiontables_curl=curl_init();
		if(preg_match("/^select|^show tables|^describe/i", $query)) {
			$url .= "?".$query;
			if($gsessionid) {
				$url .= "&gsessionid=$gsessionid";
			}
			curl_setopt($fusiontables_curl,CURLOPT_HTTPHEADER, array("Authorization: GoogleLogin auth=".$this->token));

		} else {
		if($gsessionid) {
		$url .= "?gsessionid=$gsessionid";
		}

		//set header
		curl_setopt($fusiontables_curl,CURLOPT_HTTPHEADER, array(
		"Content-length: " . strlen($query),
		"Content-type: application/x-www-form-urlencoded",
		"Authorization: GoogleLogin auth=".$this->token
		));

		//set post = true and add query to postfield
		curl_setopt($fusiontables_curl,CURLOPT_POST, true);
		curl_setopt($fusiontables_curl,CURLOPT_POSTFIELDS,$query);
		}

		curl_setopt($fusiontables_curl,CURLOPT_URL,$url);
		curl_setopt($fusiontables_curl,CURLOPT_CONNECTTIMEOUT,2);
		curl_setopt($fusiontables_curl,CURLOPT_RETURNTRANSFER,1);
		$result = curl_exec($fusiontables_curl);
		curl_close($fusiontables_curl);

		//If the result contains moved Temporarily, retry
		if (strpos($result, '302 Moved Temporarily') !== false) {
		preg_match("/(gsessionid=)([\w|-]+)/", $result, $matches);

		if (!$matches[2]) {
		return false;
		}

			if ($recursedOnce === false) {
			return $this->query($url, $matches[2], true);
		}
			return false;
			}

			return $result;
		}
}
/**
 * We wrap the Google Fusion Tables result in an object 
 *  - to parse the csv result to an array.
 *  - so it can respond to fetch() requests elsewhere in PHPActiveRecord
 */

class FusionResult extends \ArrayIterator 
{
	
	private $array;
	
	public function __construct($csv ) 
	{
		$array = self::csv_to_array($csv);
		parent::__construct( $array );
		$this->array = $array;
	}
	
	public function fetch() 
	{
		$current = $this->current();
		$this->next();
		return $current;
		
	}
	
	private static function csv_to_array($csv, $assoc = true) 
	{
		$rows = explode("\n", trim($csv));
		$array = array();
		foreach($rows as $row) {
			$array[] = str_getcsv($row, ',', '"');
		}
		if($assoc) {
			return self::array_to_assoc($array);
		}
		return $array;
	}
	private static function array_to_assoc($array) 
	{
		$columns = $array[0];
		$assoc = array();
		unset($array[0]);
		foreach($array as $row) {
			$assocrow = array();
			for($i = 0;$i < count($columns);$i++) {
				$assocrow[$columns[$i]] = $row[$i];
			}
			$assoc[] = $assocrow;
		}
		return $assoc;
	
	}
}
