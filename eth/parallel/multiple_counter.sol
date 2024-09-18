// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract MultipleCounter {

    mapping(address => uint256) public counters;

    constructor() {}

    function getCounter(address _address) public view returns (uint256) {
        return counters[_address];
    }

    function incrementForAll(address[] calldata _addresses) public {
        for (uint256 i = 0; i < _addresses.length; i++) {
            counters[_addresses[i]]++;
        }
    }

}

// Compile in Solidity 0.8.20 in https://remix.ethereum.org/
// Sig[getCounter(address)]         0xf07ec373
// Sig[incrementForAll(address[])]  0x5437ab8c