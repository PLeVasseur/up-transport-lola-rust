/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

#include "up_lola_bridge.h"

#include "score/mw/com/runtime.h"
#include "score/mw/com/types.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace
{

using score::mw::com::DataTypeMetaInfo;
using score::mw::com::EventInfo;
using score::mw::com::GenericProxy;
using score::mw::com::GenericProxyEvent;
using score::mw::com::GenericSkeleton;
using score::mw::com::GenericSkeletonEvent;
using score::mw::com::GenericSkeletonServiceElementInfo;
using score::mw::com::InstanceSpecifier;
using score::mw::com::SampleAllocateePtr;
using score::mw::com::SamplePtr;

std::optional<std::string> to_string(const UpLolaStr& value)
{
    if (value.len == 0U)
    {
        return std::string{};
    }
    if (value.data == nullptr)
    {
        return std::nullopt;
    }
    const auto* begin = reinterpret_cast<const char*>(value.data);
    return std::string{begin, value.len};
}

bool is_power_of_two(const std::size_t value)
{
    return value != 0U && (value & (value - 1U)) == 0U;
}

UpLolaStatusCode initialize_runtime_once(const std::string& config_path)
{
    if (config_path.empty())
    {
        return UP_LOLA_STATUS_OK;
    }

    static std::mutex runtime_mutex;
    static bool initialized{false};
    std::lock_guard<std::mutex> lock{runtime_mutex};
    if (initialized)
    {
        return UP_LOLA_STATUS_OK;
    }

    score::mw::com::runtime::RuntimeConfiguration runtime_configuration{config_path.c_str()};
    score::mw::com::runtime::InitializeRuntime(runtime_configuration);
    initialized = true;
    return UP_LOLA_STATUS_OK;
}

}  // namespace

struct UpLolaTxLoan
{
    SampleAllocateePtr<void> sample;
    std::size_t sample_size;
};

struct UpLolaRxSample
{
    SamplePtr<void> sample;
    std::size_t sample_size;
};

struct UpLolaTransport
{
    std::string instance_specifier;
    std::string service_type;
    std::string event_name;
    std::size_t sample_size;
    std::size_t sample_alignment;
    std::size_t max_samples;
    std::mutex mutex;
    std::optional<GenericSkeleton> skeleton;
    GenericSkeletonEvent* skeleton_event{nullptr};
    std::optional<GenericProxy> proxy;
    GenericProxyEvent* proxy_event{nullptr};
    bool subscribed{false};

    UpLolaStatusCode ensure_proxy_locked()
    {
        if (subscribed && proxy_event != nullptr)
        {
            return UP_LOLA_STATUS_OK;
        }

        auto specifier_result = InstanceSpecifier::Create(std::string{instance_specifier});
        if (!specifier_result.has_value())
        {
            return UP_LOLA_STATUS_INVALID_ARGUMENT;
        }

        auto handles_result = GenericProxy::FindService(std::move(specifier_result).value());
        if (!handles_result.has_value())
        {
            return UP_LOLA_STATUS_INTERNAL;
        }
        auto& handles = handles_result.value();
        if (handles.empty())
        {
            return UP_LOLA_STATUS_NOT_FOUND;
        }

        auto proxy_result = GenericProxy::Create(handles.front());
        if (!proxy_result.has_value())
        {
            return UP_LOLA_STATUS_INTERNAL;
        }
        proxy.emplace(std::move(proxy_result).value());

        auto& events = proxy->GetEvents();
        auto event_it = events.find(std::string_view{event_name});
        if (event_it == events.cend())
        {
            proxy.reset();
            return UP_LOLA_STATUS_NOT_FOUND;
        }
        proxy_event = &event_it->second;

        auto subscribe_result = proxy_event->Subscribe(max_samples);
        if (!subscribe_result.has_value())
        {
            proxy_event = nullptr;
            proxy.reset();
            subscribed = false;
            return UP_LOLA_STATUS_INTERNAL;
        }
        subscribed = true;
        return UP_LOLA_STATUS_OK;
    }
};

UpLolaStatusCode up_lola_transport_create(const UpLolaConfig* config, UpLolaTransport** out_transport)
{
    if (config == nullptr || out_transport == nullptr || config->sample_size == 0U ||
        !is_power_of_two(config->sample_alignment) || config->max_samples == 0U)
    {
        return UP_LOLA_STATUS_INVALID_ARGUMENT;
    }
    *out_transport = nullptr;

    auto instance_specifier = to_string(config->instance_specifier);
    auto service_type = to_string(config->service_type);
    auto event_name = to_string(config->event_name);
    auto mw_com_config_path = to_string(config->mw_com_config_path);
    if (!instance_specifier.has_value() || !service_type.has_value() || !event_name.has_value() ||
        !mw_com_config_path.has_value() || instance_specifier->empty() || service_type->empty() || event_name->empty())
    {
        return UP_LOLA_STATUS_INVALID_ARGUMENT;
    }

    const auto runtime_status = initialize_runtime_once(*mw_com_config_path);
    if (runtime_status != UP_LOLA_STATUS_OK)
    {
        return runtime_status;
    }

    auto specifier_result = InstanceSpecifier::Create(std::string{*instance_specifier});
    if (!specifier_result.has_value())
    {
        return UP_LOLA_STATUS_INVALID_ARGUMENT;
    }

    auto transport = std::make_unique<UpLolaTransport>();
    transport->instance_specifier = std::move(*instance_specifier);
    transport->service_type = std::move(*service_type);
    transport->event_name = std::move(*event_name);
    transport->sample_size = config->sample_size;
    transport->sample_alignment = config->sample_alignment;
    transport->max_samples = config->max_samples;

    const DataTypeMetaInfo data_type_meta_info{config->sample_size, config->sample_alignment};
    const std::vector<EventInfo> events{{transport->event_name, data_type_meta_info}};
    GenericSkeletonServiceElementInfo create_params;
    create_params.events = events;

    auto skeleton_result = GenericSkeleton::Create(std::move(specifier_result).value(), create_params);
    if (!skeleton_result.has_value())
    {
        return UP_LOLA_STATUS_INTERNAL;
    }
    transport->skeleton.emplace(std::move(skeleton_result).value());

    const auto& skeleton_events = transport->skeleton->GetEvents();
    auto event_it = skeleton_events.find(std::string_view{transport->event_name});
    if (event_it == skeleton_events.cend())
    {
        return UP_LOLA_STATUS_NOT_FOUND;
    }
    transport->skeleton_event = const_cast<GenericSkeletonEvent*>(&event_it->second);

    auto offer_result = transport->skeleton->OfferService();
    if (!offer_result.has_value())
    {
        return UP_LOLA_STATUS_INTERNAL;
    }

    *out_transport = transport.release();
    return UP_LOLA_STATUS_OK;
}

void up_lola_transport_destroy(UpLolaTransport* transport)
{
    if (transport == nullptr)
    {
        return;
    }
    if (transport->proxy_event != nullptr && transport->subscribed)
    {
        transport->proxy_event->Unsubscribe();
    }
    if (transport->skeleton.has_value())
    {
        transport->skeleton->StopOfferService();
    }
    delete transport;
}

UpLolaStatusCode up_lola_transport_reserve(UpLolaTransport* transport, UpLolaTxLoan** out_loan)
{
    if (transport == nullptr || out_loan == nullptr || transport->skeleton_event == nullptr)
    {
        return UP_LOLA_STATUS_INVALID_ARGUMENT;
    }
    *out_loan = nullptr;
    std::lock_guard<std::mutex> lock{transport->mutex};

    auto sample_result = transport->skeleton_event->Allocate();
    if (!sample_result.has_value())
    {
        return UP_LOLA_STATUS_RESOURCE_EXHAUSTED;
    }

    auto loan = std::make_unique<UpLolaTxLoan>();
    loan->sample = std::move(sample_result).value();
    loan->sample_size = transport->sample_size;
    auto* data = loan->sample.Get();
    if (data == nullptr)
    {
        return UP_LOLA_STATUS_INTERNAL;
    }
    std::memset(data, 0, loan->sample_size);
    *out_loan = loan.release();
    return UP_LOLA_STATUS_OK;
}

std::uint8_t* up_lola_tx_loan_data(UpLolaTxLoan* loan)
{
    if (loan == nullptr || !loan->sample)
    {
        return nullptr;
    }
    return static_cast<std::uint8_t*>(loan->sample.Get());
}

std::size_t up_lola_tx_loan_size(const UpLolaTxLoan* loan)
{
    return loan == nullptr ? 0U : loan->sample_size;
}

void up_lola_tx_loan_destroy(UpLolaTxLoan* loan)
{
    delete loan;
}

UpLolaStatusCode up_lola_transport_send(UpLolaTransport* transport, UpLolaTxLoan* loan)
{
    if (transport == nullptr || loan == nullptr || transport->skeleton_event == nullptr || !loan->sample)
    {
        return UP_LOLA_STATUS_INVALID_ARGUMENT;
    }
    std::unique_ptr<UpLolaTxLoan> owned_loan{loan};
    std::lock_guard<std::mutex> lock{transport->mutex};
    auto send_result = transport->skeleton_event->Send(std::move(owned_loan->sample));
    if (!send_result.has_value())
    {
        return UP_LOLA_STATUS_INTERNAL;
    }
    return UP_LOLA_STATUS_OK;
}

UpLolaStatusCode up_lola_transport_receive(UpLolaTransport* transport, UpLolaRxSample** out_sample)
{
    if (transport == nullptr || out_sample == nullptr)
    {
        return UP_LOLA_STATUS_INVALID_ARGUMENT;
    }
    *out_sample = nullptr;
    std::lock_guard<std::mutex> lock{transport->mutex};

    const auto proxy_status = transport->ensure_proxy_locked();
    if (proxy_status != UP_LOLA_STATUS_OK)
    {
        return proxy_status;
    }

    SamplePtr<void> received_sample;
    auto get_result = transport->proxy_event->GetNewSamples(
        [&received_sample](SamplePtr<void> sample) noexcept {
            if (!received_sample)
            {
                received_sample = std::move(sample);
            }
        },
        1U);
    if (!get_result.has_value())
    {
        return UP_LOLA_STATUS_INTERNAL;
    }
    if (*get_result == 0U || !received_sample)
    {
        return UP_LOLA_STATUS_NOT_FOUND;
    }

    auto sample = std::make_unique<UpLolaRxSample>();
    sample->sample = std::move(received_sample);
    sample->sample_size = transport->sample_size;
    *out_sample = sample.release();
    return UP_LOLA_STATUS_OK;
}

const std::uint8_t* up_lola_rx_sample_data(const UpLolaRxSample* sample)
{
    if (sample == nullptr || !sample->sample)
    {
        return nullptr;
    }
    return static_cast<const std::uint8_t*>(sample->sample.Get());
}

std::size_t up_lola_rx_sample_size(const UpLolaRxSample* sample)
{
    return sample == nullptr ? 0U : sample->sample_size;
}

void up_lola_rx_sample_destroy(UpLolaRxSample* sample)
{
    delete sample;
}
