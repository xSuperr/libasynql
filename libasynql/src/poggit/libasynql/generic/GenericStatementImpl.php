<?php

/*
 * libasynql
 *
 * Copyright (C) 2018 SOFe
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

declare(strict_types=1);

namespace poggit\libasynql\generic;

use AssertionError;
use InvalidArgumentException;
use JetBrains\PhpStorm\ArrayShape;
use JsonSerializable;
use poggit\libasynql\GenericStatement;
use poggit\libasynql\SqlDialect;
use function array_key_exists;
use function get_class;
use function gettype;
use function in_array;
use function is_object;
use function mb_strlen;
use function mb_strpos;
use function mb_substr;
use function str_replace;
use function uksort;

abstract class GenericStatementImpl implements GenericStatement, JsonSerializable{
	protected string $name;
	protected string $query;
	/** @var string */
	protected string $doc;
	/** @var GenericVariable[] */
	protected array $variables;
	protected string|array|null $file;
	protected int $lineNo;

	/** @var string[] */
	protected array $varPositions = [];

	public function getName() : string{
		return $this->name;
	}

	public function getQuery() : string{
		return $this->query;
	}

	public function getDoc() : string{
		return $this->doc;
	}

	public function getVariables() : array{
		return $this->variables;
	}

	public function getFile() : ?string{
		return $this->file;
	}

	public function getLineNumber() : int{
		return $this->lineNo;
	}

	public static function forDialect(string $dialect, string $name, string $query, string $doc, array $variables, ?string $file, int $lineNo) : GenericStatementImpl{
		static $classMap = [
			SqlDialect::MYSQL => MysqlStatementImpl::class,
			SqlDialect::SQLITE => SqliteStatementImpl::class,
		];
		$className = $classMap[$dialect];
		return new $className($name, $query, $doc, $variables, $file, $lineNo);
	}

	public function __construct(string $name, string $query, string $doc, array $variables, ?string $file, int $lineNo){
		$this->name = $name;
		$this->query = $query;
		$this->doc = $doc;
		$this->variables = $variables;
		$this->file = str_replace("\\", "/", $file);
		$this->lineNo = $lineNo;

		$this->compilePositions();
	}

	protected function compilePositions() : void{
		uksort($this->variables, function($s1, $s2){
			return mb_strlen($s2) <=> mb_strlen($s1);
		});

		$usedNames = [];

		$positions = [];
		$quotesState = null;
		for($i = 1, $iMax = mb_strlen($this->query); $i < $iMax; ++$i){
			$thisChar = mb_substr($this->query, $i, 1);

			if($quotesState !== null){
				if($thisChar === "\\"){
					++$i;
					continue;
				}
				if($thisChar === $quotesState){
					$quotesState = null;
					continue;
				}
				continue;
			}
			if(in_array($thisChar, ["'", "\"", "`"], true)){
				$quotesState = $thisChar;
				continue;
			}

			if($thisChar === ":"){
				$name = null;

				foreach($this->variables as $variable){
					if(mb_strpos($this->query, $variable->getName(), $i + 1) === $i + 1){
						$positions[$i] = $name = $variable->getName();
						break;
					}
				}

				if($name !== null){
					$usedNames[$name] = true;
					$i += mb_strlen($name);
				}
			}
		}

		$newQuery = "";
		$lastPos = 0;
		foreach($positions as $pos => $name){
			$newQuery .= mb_substr($this->query, $lastPos, $pos - $lastPos);
			$this->varPositions[mb_strlen($newQuery)] = $name;
			$lastPos = $pos + mb_strlen($name) + 1;
		}
		$newQuery .= mb_substr($this->query, $lastPos);

		$this->query = $newQuery;

		foreach($this->variables as $variable){
			if(!isset($usedNames[$variable->getName()])){
				throw new InvalidArgumentException("The variable {$variable->getName()} is not used anywhere in the query! Check for typos.");
			}
		}
	}

	public function format(array $vars, ?string $placeHolder, ?array &$outArgs) : string{
		$outArgs = [];
		foreach($this->variables as $variable){
			if(!$variable->isOptional() && !array_key_exists($variable->getName(), $vars)){
				throw new InvalidArgumentException("Missing required variable {$variable->getName()}");
			}
		}

		$query = "";

		$lastPos = 0;
		foreach($this->varPositions as $pos => $name){
			$query .= mb_substr($this->query, $lastPos, $pos - $lastPos);
			$value = $vars[$name] ?? $this->variables[$name]->getDefault();
			try{
				$query .= $this->formatVariable($this->variables[$name], $value, $placeHolder, $outArgs);
			}catch(AssertionError $e){
				throw new InvalidArgumentException("Invalid value for :$name - " . $e->getMessage() . ",  " . self::getType($value) . " given", 0, $e);
			}
			$lastPos = $pos;
		}
		$query .= mb_substr($this->query, $lastPos);

		return $query;
	}

	private static function getType($value): string
    {
		return is_object($value) ? get_class($value) : gettype($value);
	}

	protected abstract function formatVariable(GenericVariable $variable, $value, ?string $placeHolder, array &$outArgs) : string;

	#[ArrayShape(["name" => "string", "query" => "string", "doc" => "string", "variables" => "array|\poggit\libasynql\generic\GenericVariable[]", "file" => "mixed", "lineNo" => "int"])] public function jsonSerialize(): array
    {
		return [
			"name" => $this->name,
			"query" => $this->query,
			"doc" => $this->doc,
			"variables" => $this->variables,
			"file" => $this->file,
			"lineNo" => $this->lineNo,
		];
	}
}
