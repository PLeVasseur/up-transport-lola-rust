/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

#pragma once

#include <cstddef>
#include <cstdint>

extern "C" {

struct UpLolaTransport;
struct UpLolaTxLoan;
struct UpLolaRxSample;

struct UpLolaStr
{
    const std::uint8_t* data;
    std::size_t len;
};

struct UpLolaConfig
{
    UpLolaStr instance_specifier;
    UpLolaStr service_type;
    UpLolaStr event_name;
    UpLolaStr mw_com_config_path;
    std::size_t sample_size;
    std::size_t sample_alignment;
    std::size_t max_samples;
};

enum UpLolaStatusCode : std::uint32_t
{
    UP_LOLA_STATUS_OK = 0,
    UP_LOLA_STATUS_INVALID_ARGUMENT = 1,
    UP_LOLA_STATUS_NOT_FOUND = 2,
    UP_LOLA_STATUS_RESOURCE_EXHAUSTED = 3,
    UP_LOLA_STATUS_INTERNAL = 4,
};

UpLolaStatusCode up_lola_transport_create(const UpLolaConfig* config, UpLolaTransport** out_transport);
void up_lola_transport_destroy(UpLolaTransport* transport);

UpLolaStatusCode up_lola_transport_reserve(UpLolaTransport* transport, UpLolaTxLoan** out_loan);
std::uint8_t* up_lola_tx_loan_data(UpLolaTxLoan* loan);
std::size_t up_lola_tx_loan_size(const UpLolaTxLoan* loan);
void up_lola_tx_loan_destroy(UpLolaTxLoan* loan);
UpLolaStatusCode up_lola_transport_send(UpLolaTransport* transport, UpLolaTxLoan* loan);

UpLolaStatusCode up_lola_transport_receive(UpLolaTransport* transport, UpLolaRxSample** out_sample);
const std::uint8_t* up_lola_rx_sample_data(const UpLolaRxSample* sample);
std::size_t up_lola_rx_sample_size(const UpLolaRxSample* sample);
void up_lola_rx_sample_destroy(UpLolaRxSample* sample);

}
